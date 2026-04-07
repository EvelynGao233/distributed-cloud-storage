#include "storage.h"
#include "kv_client.h"
#include "utils.h"
#include <iostream>
#include <sstream>
#include <algorithm>
#include <functional>
#include <chrono>
#include <cstddef>

static KvStorageClient *storage_kv_client = nullptr;

// Chunked file layout (aligns with tablet RowKey = FileID:BlockIndex):
//   row = file_id + ":" + block_index, col = kFileBlockCol, value = up to kFileBlockSize bytes
// Legacy (pre-chunking): row = data_ns, col = file_id — still read for backward compatibility.
static constexpr size_t kFileBlockSize = 4u * 1024u * 1024u;
static const char kFileBlockCol[] = "b";

static std::string file_block_row(const std::string &file_id, size_t block_index)
{
    return file_id + ":" + std::to_string(block_index);
}

static size_t num_blocks_for_size(size_t total)
{
    if (total == 0)
        return 0;
    return (total + kFileBlockSize - 1) / kFileBlockSize;
}

// Drop on-disk bytes for this file_id (legacy monolithic cell and/or chunked rows).
static void erase_file_payload(const std::string &data_ns,
                               const std::string &meta_ns,
                               const std::string &file_id)
{
    if (!storage_kv_client)
        return;

    std::string legacy;
    if (storage_kv_client->Get(data_ns, file_id, legacy))
    {
        storage_kv_client->Delete(data_ns, file_id);
        return;
    }

    std::string size_str;
    if (!storage_kv_client->Get(meta_ns, file_id, size_str))
        return;

    size_t total = 0;
    try
    {
        total = static_cast<size_t>(std::stoull(size_str));
    }
    catch (...)
    {
        return;
    }

    for (size_t i = 0, nb = num_blocks_for_size(total); i < nb; ++i)
        storage_kv_client->Delete(file_block_row(file_id, i), kFileBlockCol);
}

static bool write_file_payload(const std::string &file_id, const std::string &content)
{
    const size_t n = num_blocks_for_size(content.size());
    for (size_t i = 0; i < n; ++i)
    {
        const size_t off = i * kFileBlockSize;
        const size_t len = std::min(kFileBlockSize, content.size() - off);
        const std::string piece(content.data() + off, len);
        if (!storage_kv_client->Put(file_block_row(file_id, i), kFileBlockCol, piece))
            return false;
    }
    return true;
}

// Reads legacy blob (data_ns, file_id) or chunked rows; returns false if missing/corrupt.
static bool read_file_payload(const std::string &data_ns,
                              const std::string &meta_ns,
                              const std::string &file_id,
                              std::string &out)
{
    out.clear();
    std::string legacy;
    if (storage_kv_client->Get(data_ns, file_id, legacy))
    {
        out = std::move(legacy);
        return true;
    }

    std::string size_str;
    if (!storage_kv_client->Get(meta_ns, file_id, size_str))
        return false;

    size_t total = 0;
    try
    {
        total = static_cast<size_t>(std::stoull(size_str));
    }
    catch (...)
    {
        return false;
    }

    if (total == 0)
        return true;

    const size_t nb = num_blocks_for_size(total);
    out.reserve(total);
    for (size_t i = 0; i < nb; ++i)
    {
        std::string piece;
        if (!storage_kv_client->Get(file_block_row(file_id, i), kFileBlockCol, piece))
            return false;
        out.append(piece);
    }

    return out.size() == total;
}

static std::string ns_files(const std::string &username)
{
    return username + "-files";
}

static std::string ns_filedata(const std::string &username)
{
    return username + "-filedata";
}

static std::string ns_filemeta(const std::string &username)
{
    return username + "-files-meta";
}

static const std::string FOLDER_MARKER = "__FOLDER__";

// Ensure the list column exists for this user's files row.
static void ensure_list_exists(const std::string &files_ns)
{
    if (!storage_kv_client) return;

    std::string dummy;

    // If the column already exists, do nothing.
    if (storage_kv_client->Get(files_ns, "list", dummy)) {
        return;
    }

    // Otherwise initialize (empty list)
    storage_kv_client->Put(files_ns, "list", "");
}

static bool update_list_with_cput(
    const std::string &files_ns,
    const std::function<void(const std::string &, std::string &)> &transform)
{
    if (!storage_kv_client)
    {
        std::cerr << "[Storage] Error: KV client not initialized in update_list_with_cput" << std::endl;
        return false;
    }

    const std::string col = "list";
    const int MAX_ATTEMPTS = 5;

    std::string current;
    storage_kv_client->Get(files_ns, col, current);

    for (int attempt = 0; attempt < MAX_ATTEMPTS; ++attempt)
    {
        std::string new_value;
        transform(current, new_value);

        if (storage_kv_client->Cput(files_ns, col, current, new_value))
        {
            return true;
        }

        if (!storage_kv_client->Get(files_ns, col, current))
        {
            current.clear();
        }
    }

    std::cerr << "[Storage] update_list_with_cput: giving up after retries" << std::endl;
    return false;
}

// Helper function: to get file list
static std::vector<std::string> split_string(const std::string &str, char delimiter)
{
    std::vector<std::string> result;
    std::stringstream ss(str);
    std::string item;

    while (std::getline(ss, item, delimiter))
    {
        if (!item.empty())
        {
            result.push_back(item);
        }
    }

    return result;
}

// Helper function: to store value
static std::string join_string(const std::vector<std::string> &vec, char delimiter)
{
    if (vec.empty())
        return "";

    std::string result = vec[0];
    for (size_t i = 1; i < vec.size(); i++)
    {
        result += delimiter;
        result += vec[i];
    }

    return result;
}

// Get the parent directory of a path
static std::string get_parent_path(const std::string &path)
{
    if (path.empty() || path == "/")
        return "/";

    std::string working_path = path;
    // Remove trailing slash if it's a folder
    if (working_path.back() == '/' && working_path.length() > 1)
    {
        working_path.pop_back();
    }

    size_t last_slash = working_path.find_last_of('/');
    if (last_slash == std::string::npos)
    {
        return "/"; // File in root directory
    }
    else if (last_slash == 0)
    {
        return "/"; // Direct child of root
    }
    else
    {
        return working_path.substr(0, last_slash + 1);
    }
}

// Get the name part of a path
static std::string get_item_name(const std::string &path)
{
    if (path.empty() || path == "/")
        return "";

    std::string working_path = path;
    // For folders, temporarily remove trailing slash
    bool is_folder = (working_path.back() == '/');
    if (is_folder && working_path.length() > 1)
    {
        working_path.pop_back();
    }

    size_t last_slash = working_path.find_last_of('/');
    std::string name;
    if (last_slash == std::string::npos)
    {
        name = working_path;
    }
    else
    {
        name = working_path.substr(last_slash + 1);
    }

    // Add back the trailing slash for folders
    if (is_folder && !name.empty())
    {
        name += '/';
    }

    return name;
}

// Normalize a path (ensure folders end with /, handle empty as root)
static std::string normalize_path(const std::string &path, bool is_folder)
{
    if (path.empty() || path == "/")
        return "/";

    std::string result = path;
    // Add leading slash if missing
    if (result[0] != '/')
    {
        result = "/" + result;
    }

    // Handle folder trailing slash
    if (is_folder)
    {
        if (result.back() != '/')
        {
            result += '/';
        }
    }
    else
    {
        // Files should not end with slash
        if (result.back() == '/')
        {
            result.pop_back();
        }
    }

    return result;
}

// Initialize the storage module with KV client
void init_storage(void *kv_client)
{
    storage_kv_client = static_cast<KvStorageClient *>(kv_client);
    std::cout << "[Storage] Storage module initialized" << std::endl;
}

bool upload_file(const std::string &username,
                 const std::string &filename,
                 const std::string &content)
{
    if (!storage_kv_client)
    {
        std::cerr << "[Storage] Error: KV client not initialized" << std::endl;
        return false;
    }

    std::string files_ns     = ns_files(username);
    std::string data_ns      = ns_filedata(username);
    std::string meta_ns      = ns_filemeta(username);
    ensure_list_exists(files_ns);

    std::string file_id;
    bool existed = storage_kv_client->Get(files_ns, filename, file_id);

    if (!existed)
    {
        file_id = "f_" + generate_token();
        
        if (!storage_kv_client->Put(files_ns, filename, file_id))
            return false;

        auto transform = [&](const std::string &old_list, std::string &new_list) {

            if (old_list.empty())
            {
                new_list = filename;
            }
            else
            {
                new_list = old_list + "," + filename;
            }
        };

        if (!update_list_with_cput(files_ns, transform))
        {
            std::cerr << "[Storage] Failed to update list via CPUT when uploading file "
                    << filename << std::endl;
            return false;
        }
    }
    else
    {
        if (file_id == FOLDER_MARKER)
        {
            std::cerr << "[Storage] Cannot upload file over an existing folder: "
                      << filename << std::endl;
            return false;
        }
    }

    erase_file_payload(data_ns, meta_ns, file_id);

    if (!write_file_payload(file_id, content))
    {
        std::cerr << "[Storage] Failed to write file data for " << filename << std::endl;
        return false;
    }

    std::string size_str = std::to_string(content.size());
    storage_kv_client->Put(meta_ns, file_id, size_str);

    std::cout << "[Storage] Uploaded file: " << filename
              << " (id = " << file_id << ", size = " << content.size() << " bytes)"
              << " for user " << username << std::endl;

    return true;
}

std::string download_file(const std::string &username, const std::string &filename)
{
    if (!storage_kv_client)
    {
        std::cerr << "[Storage] Error: KV client not initialized" << std::endl;
        return "";
    }

    std::string files_ns = ns_files(username);     // path -> file_id / FOLDER_MARKER
    std::string data_ns  = ns_filedata(username);  // file_id -> content

    auto t1 = std::chrono::steady_clock::now();

    std::string value;
    if (!storage_kv_client->Get(files_ns, filename, value))
    {
        auto t2 = std::chrono::steady_clock::now();
        std::cerr << "[Storage] File not found (no mapping): " << filename
                  << " for user " << username
                  << " via Get in "
                  << std::chrono::duration<double>(t2 - t1).count()
                  << " s" << std::endl;
        return "";
    }

    if (value == FOLDER_MARKER)
    {
        auto t2 = std::chrono::steady_clock::now();
        std::cerr << "[Storage] Path is a folder, not a file: " << filename
                  << " for user " << username
                  << " (lookup took "
                  << std::chrono::duration<double>(t2 - t1).count()
                  << " s)" << std::endl;
        return "";
    }

    std::string file_id = value;
    auto t2 = std::chrono::steady_clock::now();

    std::string meta_ns_dl = ns_filemeta(username);
    std::string content;
    if (!read_file_payload(data_ns, meta_ns_dl, file_id, content))
    {
        auto t3 = std::chrono::steady_clock::now();
        std::cerr << "[Storage] Data missing for file_id " << file_id
                  << " (path = " << filename << ") for user " << username
                  << " via Get in "
                  << std::chrono::duration<double>(t3 - t2).count()
                  << " s" << std::endl;
        return "";
    }

    auto t3 = std::chrono::steady_clock::now();

    std::cout << "[Storage] File downloaded: " << filename
              << " (id = " << file_id
              << ", " << content.size() << " bytes) for user " << username
              << " lookup="
              << std::chrono::duration<double>(t2 - t1).count()
              << " s, data_get="
              << std::chrono::duration<double>(t3 - t2).count()
              << " s" << std::endl;

    return content;
}



bool delete_file(const std::string &username, const std::string &filename)
{
    if (!storage_kv_client)
    {
        std::cerr << "[Storage] Error: KV client not initialized" << std::endl;
        return false;
    }

    std::string files_ns = ns_files(username);
    std::string data_ns  = ns_filedata(username);
    std::string meta_ns  = ns_filemeta(username);
    ensure_list_exists(files_ns);

    std::string value;
    if (!storage_kv_client->Get(files_ns, filename, value))
    {
        std::cerr << "[Storage] File mapping not found: " << filename << std::endl;
        return false;
    }

    if (value != FOLDER_MARKER)
    {
        std::string file_id = value;
        erase_file_payload(data_ns, meta_ns, file_id);
        storage_kv_client->Delete(meta_ns, file_id);
    }
    else
    {
        std::cerr << "[Storage] Warning: delete_file called on folder path: "
                  << filename << std::endl;
    }

    if (!storage_kv_client->Delete(files_ns, filename))
    {
        std::cerr << "[Storage] Failed to delete path mapping: " << filename << std::endl;
        return false;
    }

    auto transform = [&](const std::string &old_list, std::string &new_list) {
        std::vector<std::string> files = split_string(old_list, ',');
        new_list.clear();

        for (const auto &f : files)
        {
            if (f == filename)
            {
                continue;
            }
            if (!new_list.empty())
                new_list += ",";
            new_list += f;
        }
    };

    if (!update_list_with_cput(files_ns, transform))
    {
        std::cerr << "[Storage] Failed to update list via CPUT when deleting file "
                << filename << std::endl;
        return false;
    }

    std::cout << "[Storage] File deleted: " << filename << " for user " << username << std::endl;

    return true;
}


// Create a folder at a specific path
// Folder paths should end with '/'
bool create_folder(const std::string &username, const std::string &folder_name)
{
    if (!storage_kv_client)
    {
        std::cerr << "[Storage] Error: KV client not initialized" << std::endl;
        return false;
    }

    std::cout << "[Storage] Creating folder: " << folder_name << " for user " << username << std::endl;

    // Normalize folder path (ensure it ends with /)
    std::string folder_path = folder_name;
    if (!folder_path.empty() && folder_path.back() != '/')
    {
        folder_path += '/';
    }

    // Get current file list
    std::string file_list;
    storage_kv_client->Get(username + "-files", "list", file_list);

    // Check if folder already exists
    if (!file_list.empty())
    {
        std::vector<std::string> items = split_string(file_list, ',');
        for (const auto &item : items)
        {
            if (item == folder_path)
            {
                std::cout << "[Storage] Folder already exists: " << folder_path << std::endl;
                return false;
            }
        }
    }

    std::string files_ns = ns_files(username);
    ensure_list_exists(files_ns); 

    auto transform = [&](const std::string &old_list, std::string &new_list) {
        if (old_list.empty())
        {
            new_list = folder_path;
        }
        else
        {
            new_list = old_list + "," + folder_path;
        }
    };

    if (!update_list_with_cput(files_ns, transform))
    {
        std::cerr << "[Storage] Failed to update list via CPUT when creating folder "
                << folder_path << std::endl;
        return false;
    }

    // Store folder marker in path table
    storage_kv_client->Put(files_ns, folder_path, FOLDER_MARKER);

    std::cout << "[Storage] Folder created successfully: " << folder_path << std::endl;

    return true;
}

// List files and folders in a specific directory
std::vector<FileItem> list_directory(const std::string &username,
                                     const std::string &path)
{
    std::vector<FileItem> result;

    if (!storage_kv_client)
    {
        std::cerr << "[Storage] Error: KV client not initialized" << std::endl;
        return result;
    }

    // namespace
    std::string files_ns = ns_files(username);     // path -> (file_id / FOLDER_MARKER)
    std::string meta_ns  = ns_filemeta(username);  // file_id -> size

    //   "" → "/"
    //   "hw" → "hw/"
    //   "hw/" → "hw/"
    std::string dir_path = path;
    if (dir_path.empty())
        dir_path = "/";
    if (dir_path != "/" && dir_path.back() != '/')
        dir_path += '/';

    std::string file_list;
    if (!storage_kv_client->Get(files_ns, "list", file_list))
    {
        return result;
    }
    if (file_list.empty())
    {
        return result;
    }

    std::vector<std::string> all_items = split_string(file_list, ',');

    for (const auto &item : all_items)
    {
        if (dir_path == "/")
        {
            size_t slash_count = std::count(item.begin(), item.end(), '/');
            bool is_folder = (!item.empty() && item.back() == '/');

            if (is_folder)
            {
                if (slash_count == 1)
                {
                    FileItem file_item;
                    file_item.full_path = item;
                    file_item.name = item.substr(0, item.length() - 1);
                    file_item.is_folder = true;
                    file_item.size = 0;  // folder size=0
                    result.push_back(file_item);
                }
            }
            else
            {
                if (slash_count == 0)
                {
                    FileItem file_item;
                    file_item.full_path = item;
                    file_item.name = item;
                    file_item.is_folder = false;

                    std::string file_id;
                    if (storage_kv_client->Get(files_ns, item, file_id) &&
                        file_id != FOLDER_MARKER)
                    {
                        std::string size_str;
                        if (storage_kv_client->Get(meta_ns, file_id, size_str))
                        {
                            file_item.size =
                                static_cast<size_t>(std::stoull(size_str));
                        }
                        else
                        {
                            file_item.size = 0;
                        }
                    }
                    else
                    {
                        file_item.size = 0;
                    }

                    result.push_back(file_item);
                }
            }
        }
        else
        {
            if (item.find(dir_path) == 0 && item != dir_path)
            {
                std::string relative = item.substr(dir_path.length());
                size_t slash_pos = relative.find('/');
                bool is_folder = (!item.empty() && item.back() == '/');

                if (is_folder)
                {
                    if (slash_pos == relative.length() - 1)
                    {
                        FileItem file_item;
                        file_item.full_path = item;
                        file_item.name = relative.substr(0, relative.length() - 1);
                        file_item.is_folder = true;
                        file_item.size = 0;
                        result.push_back(file_item);
                    }
                }
                else
                {
                    if (slash_pos == std::string::npos)
                    {
                        FileItem file_item;
                        file_item.full_path = item;
                        file_item.name = relative;
                        file_item.is_folder = false;

                        std::string file_id;
                        if (storage_kv_client->Get(files_ns, item, file_id) &&
                            file_id != FOLDER_MARKER)
                        {
                            std::string size_str;
                            if (storage_kv_client->Get(meta_ns, file_id, size_str))
                            {
                                file_item.size =
                                    static_cast<size_t>(std::stoull(size_str));
                            }
                            else
                            {
                                file_item.size = 0;
                            }
                        }
                        else
                        {
                            file_item.size = 0;
                        }

                        result.push_back(file_item);
                    }
                }
            }
        }
    }

    std::sort(result.begin(), result.end(),
              [](const FileItem &a, const FileItem &b)
              {
                  if (a.is_folder != b.is_folder)
                  {
                      return a.is_folder; 
                  }
                  return a.name < b.name;
              });

    return result;
}


// List all files and folders
std::vector<FileItem> list_files(const std::string &username)
{
    std::vector<FileItem> result;

    if (!storage_kv_client)
    {
        std::cerr << "[Storage] Error: KV client not initialized" << std::endl;
        return result;
    }

    std::string files_ns = ns_files(username);     // path -> (file_id / FOLDER_MARKER)
    std::string meta_ns  = ns_filemeta(username);  // file_id -> size

    std::string file_list;
    if (!storage_kv_client->Get(files_ns, "list", file_list))
    {
        return result;
    }

    if (file_list.empty())
    {
        return result;
    }

    std::vector<std::string> items = split_string(file_list, ',');

    for (const auto &item : items)
    {
        FileItem file_item;
        file_item.full_path = item;
        file_item.is_folder = !item.empty() && item.back() == '/';

        if (file_item.is_folder)
        {
            file_item.name = item.substr(0, item.length() - 1);
            file_item.size = 0;
        }
        else
        {
            file_item.name = item;

            std::string file_id;
            if (storage_kv_client->Get(files_ns, item, file_id) &&
                file_id != FOLDER_MARKER)
            {
                std::string size_str;
                if (storage_kv_client->Get(meta_ns, file_id, size_str))
                {
                    file_item.size = static_cast<size_t>(std::stoull(size_str));
                }
                else
                {
                    file_item.size = 0;
                }
            }
            else
            {
                file_item.size = 0;
            }
        }

        result.push_back(file_item);
    }

    std::sort(result.begin(), result.end(),
              [](const FileItem &a, const FileItem &b)
              {
                  if (a.is_folder != b.is_folder)
                  {
                      return a.is_folder;
                  }
                  return a.name < b.name;
              });

    return result;
}


// Delete a folder and all its contents (recursive)
bool delete_folder(const std::string &username, const std::string &folder_path)
{
    if (!storage_kv_client)
    {
        std::cerr << "[Storage] Error: KV client not initialized" << std::endl;
        return false;
    }

    std::cout << "[Storage] Deleting folder: " << folder_path << " for user " << username << std::endl;

    // Normalize folder path
    std::string folder_path_normalized = folder_path;
    if (!folder_path_normalized.empty() && folder_path_normalized.back() != '/')
    {
        folder_path_normalized += '/';
    }

    // Get file list
    std::string file_list;
    if (!storage_kv_client->Get(username + "-files", "list", file_list))
    {
        return false;
    }

    std::vector<std::string> items = split_string(file_list, ',');
    std::vector<std::string> to_delete;
    std::vector<std::string> to_keep;

    // Find all items to delete (folder and its contents)
    for (const auto &item : items)
    {
        if (item == folder_path_normalized || item.find(folder_path_normalized) == 0)
        {
            to_delete.push_back(item);
        }
        else
        {
            to_keep.push_back(item);
        }
    }

    std::string files_ns = ns_files(username);
    std::string data_ns  = ns_filedata(username);
    std::string meta_ns  = ns_filemeta(username);

    // Delete all items
    for (const auto &item : to_delete)
    {
        std::string value;
        if (storage_kv_client->Get(files_ns, item, value))
        {
            if (value != FOLDER_MARKER)
            {
                std::string file_id = value;
                erase_file_payload(data_ns, meta_ns, file_id);
                storage_kv_client->Delete(meta_ns, file_id);
            }
        }

        storage_kv_client->Delete(files_ns, item);
        std::cout << "[Storage] Deleted: " << item << std::endl;
    }

    std::string new_list = join_string(to_keep, ',');

    auto transform = [&](const std::string &old_list, std::string &out_new_list) {
        out_new_list = new_list;
    };

    if (!update_list_with_cput(files_ns, transform))
    {
        std::cerr << "[Storage] Failed to update list via CPUT when deleting folder "
                  << folder_path_normalized << std::endl;
        return false;
    }

    std::cout << "[Storage] Folder deleted successfully" << std::endl;

    return true;
}


bool rename_file(const std::string &username,
                 const std::string &old_name,
                 const std::string &new_name)
{
    if (!storage_kv_client)
    {
        std::cerr << "[Storage] Error: KV client not initialized" << std::endl;
        return false;
    }

    std::string files_ns = ns_files(username);
    ensure_list_exists(files_ns); 

    std::cout << "[Storage] Renaming file from " << old_name << " to " << new_name
              << " for user " << username << std::endl;

    std::string value;
    if (!storage_kv_client->Get(files_ns, old_name, value))
    {
        std::cerr << "[Storage] File mapping not found: " << old_name << std::endl;
        return false;
    }

    if (value == FOLDER_MARKER)
    {
        std::cerr << "[Storage] rename_file called on a folder: " << old_name << std::endl;
        return false;
    }

    std::string file_id = value;

    std::string dummy;
    if (storage_kv_client->Get(files_ns, new_name, dummy))
    {
        std::cerr << "[Storage] Target path already exists: " << new_name << std::endl;
        return false;
    }

    if (!storage_kv_client->Put(files_ns, new_name, file_id))
    {
        return false;
    }

    if (!storage_kv_client->Delete(files_ns, old_name))
    {
        storage_kv_client->Delete(files_ns, new_name);
        return false;
    }

    auto transform = [&](const std::string &old_list, std::string &new_list) {
        std::vector<std::string> files = split_string(old_list, ',');
        new_list.clear();

        for (const auto &f : files)
        {
            if (!new_list.empty())
                new_list += ",";

            if (f == old_name)
            {
                new_list += new_name;
            }
            else
            {
                new_list += f;
            }
        }
    };

    if (!update_list_with_cput(files_ns, transform))
    {
        std::cerr << "[Storage] Failed to update list via CPUT when renaming file from "
                << old_name << " to " << new_name << std::endl;
        return false;
    }


    std::cout << "[Storage] File renamed successfully" << std::endl;
    return true;
}


// Rename a folder and all its contents
bool rename_folder(const std::string &username,
                   const std::string &old_folder,
                   const std::string &new_folder)
{
    if (!storage_kv_client)
    {
        std::cerr << "[Storage] Error: KV client not initialized" << std::endl;
        return false;
    }

    std::cout << "[Storage] Renaming folder from " << old_folder
              << " to " << new_folder
              << " for user " << username << std::endl;

    std::string old_folder_path = old_folder;
    std::string new_folder_path = new_folder;

    if (!old_folder_path.empty() && old_folder_path.back() != '/')
    {
        old_folder_path.push_back('/');
    }
    if (!new_folder_path.empty() && new_folder_path.back() != '/')
    {
        new_folder_path.push_back('/');
    }

    if (old_folder_path == new_folder_path)
    {
        std::cout << "[Storage] rename_folder: old == new, nothing to do" << std::endl;
        return true;
    }

    std::string files_ns = ns_files(username);

    std::vector<std::pair<std::string, std::string>> to_rename;
    bool conflict = false;
    bool folder_not_found = false;

    auto transform = [&](const std::string &old_list, std::string &out_new_list)
    {
        conflict = false;
        folder_not_found = false;
        to_rename.clear();

        if (old_list.empty())
        {
            folder_not_found = true;
            out_new_list = old_list;
            return;
        }

        std::vector<std::string> items = split_string(old_list, ',');
        std::vector<std::string> new_items;
        new_items.reserve(items.size());

        for (const auto &item : items)
        {
            bool under_new = (item.compare(0, new_folder_path.size(), new_folder_path) == 0);
            bool under_old = (item.compare(0, old_folder_path.size(), old_folder_path) == 0);

            if (under_new && !under_old)
            {
                conflict = true;
                out_new_list = old_list; // 不改 list
                return;
            }
        }

        bool saw_old = false;

        for (const auto &item : items)
        {
            if (item == old_folder_path)
            {
                saw_old = true;
                to_rename.push_back({item, new_folder_path});
                new_items.push_back(new_folder_path);
            }
            else if (item.compare(0, old_folder_path.size(), old_folder_path) == 0)
            {
                saw_old = true;
                std::string suffix = item.substr(old_folder_path.size());
                std::string new_item = new_folder_path + suffix;
                to_rename.push_back({item, new_item});
                new_items.push_back(new_item);
            }
            else
            {
                new_items.push_back(item);
            }
        }

        if (!saw_old)
        {
            folder_not_found = true;
            to_rename.clear();
            out_new_list = old_list;
            return;
        }

        out_new_list = join_string(new_items, ',');
    };

    if (!update_list_with_cput(files_ns, transform))
    {
        std::cerr << "[Storage] rename_folder: update_list_with_cput failed (retries exceeded)"
                  << std::endl;
        return false;
    }

    if (conflict)
    {
        std::cerr << "[Storage] rename_folder: target path already exists under "
                  << new_folder_path << std::endl;
        return false;
    }

    if (folder_not_found)
    {
        std::cerr << "[Storage] rename_folder: folder not found: "
                  << old_folder_path << std::endl;
        return false;
    }

    for (const auto &[old_key, new_key] : to_rename)
    {
        std::string value;
        if (storage_kv_client->Get(files_ns, old_key, value))
        {
            storage_kv_client->Put(files_ns, new_key, value);
            storage_kv_client->Delete(files_ns, old_key);

            std::cout << "[Storage] Renamed mapping: " << old_key
                      << " -> " << new_key << std::endl;
        }
        else
        {
            std::cerr << "[Storage] rename_folder: mapping not found for key "
                      << old_key << std::endl;
        }
    }

    std::cout << "[Storage] Folder renamed successfully from "
              << old_folder_path << " to " << new_folder_path << std::endl;
    return true;
}



// Move a file to a different folder
bool move_file(const std::string &username,
               const std::string &file_path,
               const std::string &dest_folder)
{
    if (!storage_kv_client)
    {
        std::cerr << "[Storage] Error: KV client not initialized" << std::endl;
        return false;
    }

    std::string files_ns = ns_files(username);
    ensure_list_exists(files_ns);

    std::cout << "[Storage] Moving file " << file_path << " to " << dest_folder
              << " for user " << username << std::endl;

    // Ensure destination folder path ends with /
    std::string dest_folder_path = dest_folder;
    if (!dest_folder_path.empty() && dest_folder_path != "/" &&
        dest_folder_path.back() != '/')
    {
        dest_folder_path += '/';
    }

    // Extract filename from path
    std::string filename = get_item_name(file_path);
    if (filename.empty())
    {
        std::cerr << "[Storage] Invalid file path: " << file_path << std::endl;
        return false;
    }

    // Build new path
    std::string new_path;
    if (dest_folder_path.empty() || dest_folder_path == "/")
    {
        new_path = filename; // Moving to root
    }
    else
    {
        new_path = dest_folder_path + filename;
    }

    // If source and destination are the same, return
    if (file_path == new_path)
    {
        std::cout << "[Storage] Source and destination are the same" << std::endl;
        return true;
    }

    std::string value;
    if (!storage_kv_client->Get(files_ns, file_path, value))
    {
        std::cerr << "[Storage] File mapping not found: " << file_path << std::endl;
        return false;
    }
    if (value == FOLDER_MARKER)
    {
        std::cerr << "[Storage] move_file called on folder path: " << file_path << std::endl;
        return false;
    }
    std::string file_id = value;

    if (!dest_folder_path.empty() && dest_folder_path != "/")
    {
        std::string folder_val;
        if (!storage_kv_client->Get(files_ns, dest_folder_path, folder_val) ||
            folder_val != FOLDER_MARKER)
        {
            std::cerr << "[Storage] Destination folder not found: "
                      << dest_folder_path << std::endl;
            return false;
        }
    }

    std::string tmp;
    if (storage_kv_client->Get(files_ns, new_path, tmp))
    {
        std::cerr << "[Storage] Target path already exists: " << new_path << std::endl;
        return false;
    }

    if (!storage_kv_client->Put(files_ns, new_path, file_id))
    {
        return false;
    }

    if (!storage_kv_client->Delete(files_ns, file_path))
    {
        storage_kv_client->Delete(files_ns, new_path);
        return false;
    }
    auto transform = [&](const std::string &old_list, std::string &new_list) {
        std::vector<std::string> files = split_string(old_list, ',');
        new_list.clear();

        for (const auto &f : files)
        {
            if (!new_list.empty())
                new_list += ",";

            if (f == file_path)
            {
                new_list += new_path;
            }
            else
            {
                new_list += f;
            }
        }
    };

    if (!update_list_with_cput(files_ns, transform))
    {
        std::cerr << "[Storage] Failed to update list via CPUT when moving file "
                << file_path << " -> " << new_path << std::endl;
        return false;
    }

    std::cout << "[Storage] File moved successfully: " << file_path
              << " -> " << new_path << std::endl;
    return true;
}

// Move a folder to a different location
bool move_folder(const std::string &username, const std::string &folder_path,
                 const std::string &dest_folder)
{
    if (!storage_kv_client)
    {
        std::cerr << "[Storage] Error: KV client not initialized" << std::endl;
        return false;
    }

    std::cout << "[Storage] Moving folder " << folder_path << " to " << dest_folder
              << " for user " << username << std::endl;

    // Ensure paths end with /
    std::string src_folder_path = folder_path;
    std::string dest_folder_path = dest_folder;

    if (!src_folder_path.empty() && src_folder_path.back() != '/')
    {
        src_folder_path += '/';
    }
    if (!dest_folder_path.empty() && dest_folder_path != "/" && dest_folder_path.back() != '/')
    {
        dest_folder_path += '/';
    }

    // Extract folder name
    std::string folder_name = get_item_name(src_folder_path);
    if (folder_name.empty())
    {
        std::cerr << "[Storage] Invalid folder path: " << src_folder_path << std::endl;
        return false;
    }

    // Build new path
    std::string new_folder_path;
    if (dest_folder_path.empty() || dest_folder_path == "/")
    {
        new_folder_path = folder_name; // Moving to root
    }
    else
    {
        new_folder_path = dest_folder_path + folder_name;
    }

    // Check if trying to move folder into itself
    if (!dest_folder_path.empty() &&
        dest_folder_path.find(src_folder_path) == 0)
    {
        std::cerr << "[Storage] Cannot move folder into itself" << std::endl;
        return false;
    }

    // If source and destination are the same, return
    if (src_folder_path == new_folder_path)
    {
        std::cout << "[Storage] Source and destination are the same" << std::endl;
        return true;
    }

    // Get file list
    std::string file_list;
    if (!storage_kv_client->Get(username + "-files", "list", file_list))
    {
        return false;
    }

    std::vector<std::string> items = split_string(file_list, ',');
    std::vector<std::string> new_items;
    std::vector<std::pair<std::string, std::string>> to_move;

    // Collect items to move
    for (const auto &item : items)
    {
        if (item == src_folder_path)
        {
            // The folder itself
            to_move.push_back({item, new_folder_path});
            new_items.push_back(new_folder_path);
        }
        else if (item.find(src_folder_path) == 0)
        {
            // Content inside folder
            std::string relative_path = item.substr(src_folder_path.length());
            std::string new_item = new_folder_path + relative_path;
            to_move.push_back({item, new_item});
            new_items.push_back(new_item);
        }
        else
        {
            new_items.push_back(item);
        }
    }

    if (to_move.empty())
    {
        std::cerr << "[Storage] Folder not found: " << src_folder_path << std::endl;
        return false;
    }

    std::string files_ns = ns_files(username);

    // Perform move operations
    for (const auto &[old_key, new_key] : to_move)
    {
        std::string value;
        if (storage_kv_client->Get(files_ns, old_key, value))
        {
            storage_kv_client->Put(files_ns, new_key, value);
            storage_kv_client->Delete(files_ns, old_key);
            std::cout << "[Storage] Moved mapping: " << old_key
                      << " -> " << new_key << std::endl;
        }
    }

    std::string new_list = join_string(new_items, ',');

    auto transform = [&](const std::string &old_list, std::string &out_new_list) {
        out_new_list = new_list;
    };

    if (!update_list_with_cput(files_ns, transform))
    {
        std::cerr << "[Storage] Failed to update list via CPUT when moving folder "
                  << src_folder_path << " to " << new_folder_path << std::endl;
        return false;
    }

    std::cout << "[Storage] Folder moved successfully" << std::endl;
    return true;
}

bool append_file(const std::string &username,
                 const std::string &filename,
                 const std::string &chunk)
{
    if (!storage_kv_client)
        return false;

    std::string files_ns = ns_files(username);
    std::string data_ns  = ns_filedata(username);
    std::string meta_ns  = ns_filemeta(username);

    std::string value;
    if (!storage_kv_client->Get(files_ns, filename, value))
    {
        std::cerr << "[Storage] append_file: path not found: " << filename << std::endl;
        return false;
    }
    if (value == FOLDER_MARKER)
    {
        std::cerr << "[Storage] append_file: cannot append to folder: " << filename << std::endl;
        return false;
    }
    std::string file_id = value;

    std::string old_content;
    if (!read_file_payload(data_ns, meta_ns, file_id, old_content))
        old_content.clear();

    old_content += chunk;

    erase_file_payload(data_ns, meta_ns, file_id);

    if (!write_file_payload(file_id, old_content))
        return false;

    std::string size_str = std::to_string(old_content.size());
    storage_kv_client->Put(meta_ns, file_id, size_str);

    return true;
}
