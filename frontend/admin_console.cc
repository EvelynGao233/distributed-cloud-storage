// admin_console.cc
#include "admin_console.h"

#include <grpcpp/grpcpp.h>
#include <fstream>
#include <sstream>
#include <vector>
#include <memory>
#include <iostream>

#include "myproto/kvstorage.grpc.pb.h"
#include "myproto/coordinator.grpc.pb.h"

extern std::string url_encode(const std::string &str);

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using kvstorage::Coordinator;
using kvstorage::GetClusterStatusRequest;
using kvstorage::ClusterStatusResponse;
using kvstorage::NodeStatus;
using kvstorage::PartitionStatus;
using kvstorage::KvStorage;
using kvstorage::RawDataRequest;
using kvstorage::RawDataResponse;
using kvstorage::ControlRequest;
using kvstorage::ControlResponse;

static const char *kDefaultCoordinatorAddr = "localhost:5051";

static std::unique_ptr<Coordinator::Stub> g_coord_stub;

// Node Type Detection Functions
static bool is_kv_storage_node(const std::string &addr)
{
    return addr.find(":808") != std::string::npos;
}

static bool is_frontend_node(const std::string &addr)
{
    return addr.find(":900") != std::string::npos;
}

static bool is_smtp_node(const std::string &addr)
{
    return addr.find(":25") != std::string::npos ||
           addr.find(":110") != std::string::npos;
}

// Utility Functions
static std::string read_file_to_string(const std::string &path)
{
    std::ifstream in(path);
    if (!in)
    {
        std::ostringstream oss;
        oss << "<html><body><h1>Cannot open " << path << "</h1></body></html>";
        return oss.str();
    }
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

static void replace_all(std::string &str,
                        const std::string &from,
                        const std::string &to)
{
    if (from.empty())
        return;
    size_t pos = 0;
    while ((pos = str.find(from, pos)) != std::string::npos)
    {
        str.replace(pos, from.length(), to);
        pos += to.length();
    }
}

static std::string html_escape(const std::string &in)
{
    std::string out;
    out.reserve(in.size());
    for (char c : in)
    {
        switch (c)
        {
        case '&':
            out += "&amp;";
            break;
        case '<':
            out += "&lt;";
            break;
        case '>':
            out += "&gt;";
            break;
        case '"':
            out += "&quot;";
            break;
        case '\'':
            out += "&#39;";
            break;
        default:
            out.push_back(c);
            break;
        }
    }
    return out;
}

static void ensure_coord_stub()
{
    if (!g_coord_stub)
    {
        auto channel = grpc::CreateChannel(
            kDefaultCoordinatorAddr,
            grpc::InsecureChannelCredentials());
        g_coord_stub = Coordinator::NewStub(channel);
        std::cout << "[Admin] Connected to Coordinator at "
                  << kDefaultCoordinatorAddr << std::endl;
    }
}

// Table Builders
static void build_cluster_tables(const ClusterStatusResponse &resp,
                                 std::string &node_rows,
                                 std::string &partition_rows,
                                 std::string &node_options,
                                 std::string &selected_node)
{
    // Build Node Table
    for (const auto &node : resp.nodes())
    {
        const std::string &addr = node.address();
        
        // Skip SMTP nodes
        if (is_smtp_node(addr))
        {
            continue;
        }
        
        bool alive = node.is_alive();
        long long hb_ms = node.last_heartbeat_ms();

        node_rows += "<tr>";
        node_rows += "<td>" + html_escape(addr) + "</td>";
        node_rows += "<td>";
        if (alive)
        {
            node_rows += "<span class='badge badge-alive text-white'>ALIVE</span>";
        }
        else
        {
            node_rows += "<span class='badge badge-dead text-white'>DEAD</span>";
        }
        node_rows += "</td>";
        node_rows += "<td>" + std::to_string(hb_ms) + "</td>";
        node_rows += "</tr>";

        // Node selector - KV and Frontend nodes only
        if (is_kv_storage_node(addr) || is_frontend_node(addr))
        {
            node_options += "<option value='" + html_escape(addr) + "'";
            if (selected_node.empty())
            {
                selected_node = addr;
                node_options += " selected";
            }
            else if (selected_node == addr)
            {
                node_options += " selected";
            }
            node_options += ">";
            node_options += html_escape(addr);
            if (alive)
                node_options += " (ALIVE)";
            else
                node_options += " (DEAD)";
            node_options += "</option>";
        }
    }

    // Build Partition Table - KV storage nodes only
    for (const auto &p : resp.partitions())
    {
        if (!is_kv_storage_node(p.primary()))
        {
            continue;
        }

        partition_rows += "<tr>";
        partition_rows += "<td>" + std::to_string(p.view_id()) + "</td>";

        std::string range = "[" +
                            (p.start().empty() ? std::string("-inf") : html_escape(p.start())) +
                            ", " +
                            (p.end().empty() ? std::string("+inf") : html_escape(p.end())) +
                            ")";
        partition_rows += "<td>" + range + "</td>";

        partition_rows += "<td>" + html_escape(p.primary()) + "</td>";

        std::string backups;
        for (int i = 0; i < p.backups_size(); i++)
        {
            if (i > 0)
                backups += "<br/>";
            backups += html_escape(p.backups(i));
        }
        partition_rows += "<td>" + backups + "</td>";

        partition_rows += "</tr>";
    }
}

static void build_raw_data_table(const std::string &node_addr,
                                 const std::string &start_key,
                                 std::string &rows_html,
                                 std::string &pagination_html,
                                 std::string &message)
{
    if (is_smtp_node(node_addr))
    {
        rows_html = "<tr><td colspan='3' class='text-muted'>SMTP nodes do not store KV data.</td></tr>";
        pagination_html = "<span class='text-muted'>N/A</span>";
        return;
    }

    if (is_frontend_node(node_addr))
    {
        rows_html = "<tr><td colspan='3' class='text-muted'>Frontend nodes do not store KV data directly. Select a KV storage node to view data.</td></tr>";
        pagination_html = "<span class='text-muted'>N/A</span>";
        return;
    }

    if (node_addr.empty())
    {
        message = "No node selected.";
        return;
    }

    grpc::ChannelArguments args;
    args.SetMaxReceiveMessageSize(25 * 1024 * 1024);
    args.SetMaxSendMessageSize(25 * 1024 * 1024);

    auto channel = grpc::CreateCustomChannel(
        node_addr,
        grpc::InsecureChannelCredentials(),
        args
    );
    auto stub = KvStorage::NewStub(channel);

    RawDataRequest req;
    if (!start_key.empty())
    {
        req.set_start_key(start_key);
    }
    req.set_limit(20);

    RawDataResponse resp;
    ClientContext ctx;
    Status s = stub->GetRawData(&ctx, req, &resp);
    if (!s.ok())
    {
        std::ostringstream oss;
        oss << "GetRawData RPC failed for node " << node_addr
            << ": " << s.error_message();
        message = oss.str();
        return;
    }

    if (resp.entries_size() == 0)
    {
        rows_html = "<tr><td colspan='3' class='text-muted'>No entries.</td></tr>";
    }
    else
    {
        for (const auto &e : resp.entries())
        {
            rows_html += "<tr>";
            rows_html += "<td class='raw-data-key'>" + html_escape(e.key()) + "</td>";
            rows_html += "<td class='raw-data-key'>" + html_escape(e.col()) + "</td>";
            rows_html += "<td class='raw-data-val'>" + html_escape(e.val()) + "</td>";
            rows_html += "</tr>";
        }
    }

    if (resp.has_more())
    {
        std::string next_key = resp.next_start_key();
        std::string url = "/admin?node=" + url_encode(node_addr) +
                          "&start_key=" + url_encode(next_key);
        pagination_html =
            "<a href='" + url + "' class='btn btn-sm btn-outline-primary'>Next page</a>";
    }
    else
    {
        pagination_html =
            "<span class='text-muted'>End of data</span>";
    }
}

// Page Renderer
std::string render_admin_page(const std::string &username,
                              const std::string &selected_node_input,
                              const std::string &start_key,
                              const std::string &message_input)
{
    std::string html = read_file_to_string("pages/admin.html");

    replace_all(html, "<!-- USERNAME_PLACEHOLDER -->", html_escape(username));

    ensure_coord_stub();

    ClusterStatusResponse status_resp;
    {
        GetClusterStatusRequest req;
        ClientContext ctx;
        Status s = g_coord_stub->GetClusterStatus(&ctx, req, &status_resp);
        if (!s.ok())
        {
            std::string msg = "GetClusterStatus RPC failed: " + s.error_message();
            std::string safe_msg = "<div class='alert alert-danger mb-0' role='alert'>" +
                                   html_escape(msg) + "</div>";
            replace_all(html, "<!-- NODE_TABLE_PLACEHOLDER -->",
                        "<tr><td colspan='3' class='text-danger'>Failed to fetch cluster status</td></tr>");
            replace_all(html, "<!-- PARTITION_TABLE_PLACEHOLDER -->", "");
            replace_all(html, "<!-- NODE_OPTIONS_PLACEHOLDER -->", "");
            replace_all(html, "<!-- RAW_DATA_TABLE_PLACEHOLDER -->", "");
            replace_all(html, "<!-- PAGINATION_PLACEHOLDER -->", "");
            replace_all(html, "<!-- ADMIN_MESSAGE_PLACEHOLDER -->", safe_msg);
            return html;
        }
    }

    std::string node_rows;
    std::string partition_rows;
    std::string node_options;
    std::string selected_node = selected_node_input;

    if (selected_node.empty() && status_resp.nodes_size() > 0)
    {
        // Try to select first KV storage node
        for (const auto &node : status_resp.nodes())
        {
            if (is_kv_storage_node(node.address()))
            {
                selected_node = node.address();
                break;
            }
        }
        // If no KV nodes, select first frontend node
        if (selected_node.empty())
        {
            for (const auto &node : status_resp.nodes())
            {
                if (is_frontend_node(node.address()))
                {
                    selected_node = node.address();
                    break;
                }
            }
        }
    }

    build_cluster_tables(status_resp, node_rows, partition_rows, node_options, selected_node);

    if (status_resp.nodes_size() == 0)
    {
        node_rows = "<tr><td colspan='3' class='text-muted'>No nodes registered.</td></tr>";
    }

    replace_all(html, "<!-- NODE_TABLE_PLACEHOLDER -->", node_rows);
    replace_all(html, "<!-- PARTITION_TABLE_PLACEHOLDER -->", partition_rows);
    replace_all(html, "<!-- NODE_OPTIONS_PLACEHOLDER -->", node_options.empty()
                                                                ? "<option value=''>No nodes</option>"
                                                                : node_options);

    std::string raw_rows;
    std::string pagination_html;
    std::string msg = message_input;

    if (status_resp.nodes_size() == 0)
    {
        raw_rows = "<tr><td colspan='3' class='text-muted'>No KV nodes available.</td></tr>";
        pagination_html.clear();
        if (msg.empty())
            msg = "No KV nodes in the cluster.";
    }
    else
    {
        std::string data_node = selected_node;
        if ((data_node == "__all__" || data_node.empty()) && status_resp.nodes_size() > 0)
        {
            for (const auto &node : status_resp.nodes())
            {
                if (is_kv_storage_node(node.address()))
                {
                    data_node = node.address();
                    break;
                }
            }
        }

        build_raw_data_table(data_node, start_key, raw_rows, pagination_html, msg);
        if (raw_rows.empty())
        {
            raw_rows = "<tr><td colspan='3' class='text-muted'>No entries.</td></tr>";
        }
    }

    replace_all(html, "<!-- RAW_DATA_TABLE_PLACEHOLDER -->", raw_rows);
    replace_all(html, "<!-- PAGINATION_PLACEHOLDER -->", pagination_html);

    if (!msg.empty())
    {
        std::string alert_class = msg.find("failed") != std::string::npos ||
                                          msg.find("Failed") != std::string::npos
                                      ? "alert-danger"
                                      : "alert-info";
        std::string safe_msg = "<div class='alert " + alert_class +
                               " mb-0' role='alert'>" + html_escape(msg) + "</div>";
        replace_all(html, "<!-- ADMIN_MESSAGE_PLACEHOLDER -->", safe_msg);
    }
    else
    {
        replace_all(html, "<!-- ADMIN_MESSAGE_PLACEHOLDER -->", "");
    }

    return html;
}

// Command Executor (returns message only)
std::string execute_admin_command(const std::string &node,
                                   const std::string &cmd)
{
    std::string msg;
    auto canonical_cmd = [&](const std::string &raw) -> std::string
    {
        std::string t = raw;
        while (!t.empty() && isspace(static_cast<unsigned char>(t.front())))
            t.erase(t.begin());
        while (!t.empty() && isspace(static_cast<unsigned char>(t.back())))
            t.pop_back();

        if (t.rfind("shutdown", 0) == 0)
            return "shutdown";
        if (t.rfind("restart", 0) == 0)
            return "restart";
        return t;
    };
    std::string cmd_norm = canonical_cmd(cmd);

    if (node.empty())
    {
        return "Please choose a node.";
    }

    auto invoke_cmd = [&](const std::string &target, std::string &out_msg)
    {
        auto channel = grpc::CreateChannel(
            target, grpc::InsecureChannelCredentials());
        auto stub = KvStorage::NewStub(channel);

        ControlRequest req;
        ControlResponse resp;

        if (cmd_norm == "shutdown")
        {
            req.set_cmd(ControlRequest::SHUTDOWN);
        }
        else if (cmd_norm == "restart")
        {
            req.set_cmd(ControlRequest::RESTART);
        }
        else
        {
            out_msg = "Unknown command: " + cmd_norm;
            return false;
        }

        ClientContext ctx;
        Status s = stub->AdminControl(&ctx, req, &resp);
        if (!s.ok())
        {
            out_msg = "AdminControl RPC failed for " + target + ": " + s.error_message();
            return false;
        }
        if (!resp.success())
        {
            out_msg = "AdminControl on " + target + " failed: " + resp.message();
            return false;
        }

        out_msg = "Admin command '" + cmd_norm + "' sent to " + target + ": " + resp.message();
        return true;
    };

    std::vector<std::string> results;
    int success_count = 0;

    if (node == "__all__")
    {
        ensure_coord_stub();
        GetClusterStatusRequest req;
        ClusterStatusResponse status_resp;
        ClientContext ctx;
        Status s = g_coord_stub->GetClusterStatus(&ctx, req, &status_resp);
        if (!s.ok())
        {
            return "GetClusterStatus RPC failed: " + s.error_message();
        }

        for (const auto &n : status_resp.nodes())
        {
            if (is_smtp_node(n.address()))
            {
                continue;
            }
            
            std::string res_msg;
            if (invoke_cmd(n.address(), res_msg))
            {
                success_count++;
            }
            results.push_back(res_msg);
        }

        std::ostringstream oss;
        oss << "Sent '" << cmd << "' to " << results.size()
            << " nodes. Success: " << success_count;
        for (const auto &r : results)
        {
            oss << "<br/>" << html_escape(r);
        }
        msg = oss.str();
    }
    else
    {
        std::string res_msg;
        invoke_cmd(node, res_msg);
        msg = res_msg;
    }

    return msg;
}

// Command Handler
std::string handle_admin_command(const std::string &username,
                                 const std::string &node,
                                 const std::string &cmd)
{
    std::string msg = execute_admin_command(node, cmd);
    return render_admin_page(username, node, "", msg);
}