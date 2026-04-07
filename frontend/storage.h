#ifndef STORAGE_H
#define STORAGE_H

#include <string>
#include <vector>

// Structure to represent a file or folder item
struct FileItem {
    std::string name;       // Display name (without path)
    std::string full_path;  // Full path including folders
    bool is_folder;         // True if it's a folder
    size_t size;           // File size in bytes (0 for folders)
};

// Initialize the storage module with KV client
void init_storage(void* kv_client);

// File operations
bool upload_file(const std::string& username, const std::string& filename, const std::string& content);
bool append_file(const std::string& username,const std::string& filename, const std::string& chunk);
std::string download_file(const std::string& username, const std::string& filename);

bool delete_file(const std::string& username, const std::string& filename);
bool rename_file(const std::string& username, const std::string& old_name, const std::string& new_name);
bool move_file(const std::string& username, const std::string& file_path, const std::string& dest_folder);

// Folder operations  
bool create_folder(const std::string& username, const std::string& folder_name);
bool delete_folder(const std::string& username, const std::string& folder_path);
bool rename_folder(const std::string& username, const std::string& old_folder, const std::string& new_folder);
bool move_folder(const std::string& username, const std::string& folder_path, const std::string& dest_folder);

// Listing operations
std::vector<FileItem> list_files(const std::string& username);
std::vector<FileItem> list_directory(const std::string& username, const std::string& path);

#endif // STORAGE_H