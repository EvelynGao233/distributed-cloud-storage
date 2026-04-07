// frontend_node_registration.cc
#include "frontend_node_registration.h"

#include <grpcpp/grpcpp.h>
#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <mutex>

#include "myproto/coordinator.grpc.pb.h"
#include "myproto/kvstorage.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;

using kvstorage::Coordinator;
using kvstorage::RegisterRequest;
using kvstorage::RegisterResponse;
using kvstorage::HeartbeatRequest;
using kvstorage::HeartbeatResponse;
using kvstorage::KvStorage;
using kvstorage::ControlRequest;
using kvstorage::ControlResponse;

namespace
{
    std::unique_ptr<Coordinator::Stub> g_coord_stub;
    std::string g_my_addr;
    std::string g_coordinator_addr;
    std::atomic<bool> g_hb_running{false};
    std::once_flag g_start_once;
    
    std::unique_ptr<Server> g_grpc_server;
    std::thread g_grpc_thread;
    
    std::atomic<bool> g_server_should_run{true};
    
    std::mutex g_hb_mutex;

    void heartbeat_loop()
    {
        std::cout << "[FrontendNode] Heartbeat loop started, thread_id=" 
                  << std::this_thread::get_id() << std::endl;
        
        while (g_hb_running.load())
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));

            if (!g_coord_stub)
                continue;

            HeartbeatRequest req;
            req.mutable_node()->set_address(g_my_addr);
            req.set_known_view_version(0);

            HeartbeatResponse resp;
            ClientContext ctx;
            Status s = g_coord_stub->Heartbeat(&ctx, req, &resp);
            if (!s.ok())
            {
                std::cerr << "[FrontendNode] Heartbeat failed: "
                          << s.error_message() << std::endl;
            }
        }
        std::cout << "[FrontendNode] Heartbeat loop exiting, thread_id=" 
                  << std::this_thread::get_id() << std::endl;
    }
    
    void restart_heartbeat()
    {
        std::cout << "[FrontendNode] restart_heartbeat called, thread_id=" 
                  << std::this_thread::get_id() << std::endl;
        
        std::lock_guard<std::mutex> lock(g_hb_mutex);
        
        if (g_hb_running.load())
        {
            std::cout << "[FrontendNode] Heartbeat already running, skipping" << std::endl;
            return;
        }
        
        std::cout << "[FrontendNode] Attempting to re-register..." << std::endl;
        
        if (g_coord_stub)
        {
            RegisterRequest req;
            req.mutable_node()->set_address(g_my_addr);
            RegisterResponse resp;
            ClientContext ctx;
            
            Status s = g_coord_stub->Register(&ctx, req, &resp);
            if (s.ok() && resp.success())
            {
                std::cout << "[FrontendNode] Re-registered with coordinator" << std::endl;
            }
            else
            {
                std::cerr << "[FrontendNode] Re-registration failed: " 
                          << s.error_message() << std::endl;
            }
        }
        
        std::cout << "[FrontendNode] Setting g_hb_running to true" << std::endl;
        g_hb_running.store(true);
        
        std::cout << "[FrontendNode] Creating new heartbeat thread..." << std::endl;
        std::thread hb_thread(heartbeat_loop);
        hb_thread.detach();
        
        std::cout << "[FrontendNode] Heartbeat thread created and detached" << std::endl;
    }

    class FrontendControlService final : public KvStorage::Service
    {
    public:
        Status AdminControl(ServerContext* context,
                          const ControlRequest* request,
                          ControlResponse* response) override
        {
            std::cout << "[FrontendNode] AdminControl called, thread_id=" 
                      << std::this_thread::get_id() << std::endl;
            std::cout << "[FrontendNode] Received admin command: ";
            
            if (request->cmd() == ControlRequest::SHUTDOWN)
            {
                std::cout << "SHUTDOWN" << std::endl;
                response->set_success(true);
                response->set_message("Frontend node shutting down (simulated)...");
                
                std::cout << "[FrontendNode] Setting g_server_should_run to false" << std::endl;
                g_server_should_run.store(false);
                
                std::cout << "[FrontendNode] Setting g_hb_running to false" << std::endl;
                g_hb_running.store(false);
                
                std::cout << "[FrontendNode] Stopped accepting connections and heartbeat" << std::endl;
            }
            else if (request->cmd() == ControlRequest::RESTART)
            {
                std::cout << "RESTART" << std::endl;
                response->set_success(true);
                response->set_message("Frontend node restarting...");
                
                std::cout << "[FrontendNode] Setting g_server_should_run to true" << std::endl;
                g_server_should_run.store(true);
                
                std::cout << "[FrontendNode] Sleeping 100ms..." << std::endl;
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                
                std::cout << "[FrontendNode] Creating restart thread..." << std::endl;
                std::thread restart_thread(restart_heartbeat);
                restart_thread.detach();
                
                std::cout << "[FrontendNode] Resumed accepting connections" << std::endl;
            }
            else
            {
                response->set_success(false);
                response->set_message("Unknown command");
            }
            
            std::cout << "[FrontendNode] AdminControl returning OK" << std::endl;
            return Status::OK;
        }
    };

}

void start_frontend_registration(const std::string &my_address,
                                 const std::string &coordinator_addr)
{
    std::call_once(g_start_once, [&]() {
        g_my_addr = my_address;
        g_coordinator_addr = coordinator_addr;

        auto channel = grpc::CreateChannel(
            coordinator_addr,
            grpc::InsecureChannelCredentials());
        g_coord_stub = Coordinator::NewStub(channel);

        RegisterRequest req;
        req.mutable_node()->set_address(g_my_addr);

        RegisterResponse resp;
        ClientContext ctx;

        Status s = g_coord_stub->Register(&ctx, req, &resp);

        if (!s.ok() || !resp.success())
        {
            std::cerr << "[FrontendNode] Register with coordinator failed: "
                      << s.error_message() << std::endl;
        }
        else
        {
            std::cout << "[FrontendNode] Registered with coordinator as "
                      << g_my_addr << std::endl;
        }

        g_hb_running.store(true);
        std::thread(heartbeat_loop).detach();
    });
}

void stop_frontend_registration()
{
    std::cout << "[FrontendNode] stop_frontend_registration called" << std::endl;
    g_hb_running.store(false);
    std::cout << "[FrontendNode] Stopped heartbeat" << std::endl;
}

void start_frontend_grpc_control_server(const std::string &listen_addr)
{
    static std::once_flag start_once;
    std::call_once(start_once, [&]() {
        std::cout << "[FrontendNode] Starting gRPC control server..." << std::endl;
        g_grpc_thread = std::thread([listen_addr]() {
            std::cout << "[FrontendNode] gRPC server thread started, thread_id=" 
                      << std::this_thread::get_id() << std::endl;
            
            FrontendControlService service;
            ServerBuilder builder;
            
            builder.AddListeningPort(listen_addr, grpc::InsecureServerCredentials());
            builder.RegisterService(&service);
            
            g_grpc_server = builder.BuildAndStart();
            
            if (g_grpc_server)
            {
                std::cout << "[FrontendNode] gRPC control server listening on "
                          << listen_addr << std::endl;
                std::cout << "[FrontendNode] gRPC server calling Wait()..." << std::endl;
                g_grpc_server->Wait();
                std::cout << "[FrontendNode] gRPC server Wait() returned!" << std::endl;
            }
            else
            {
                std::cerr << "[FrontendNode] Failed to start gRPC server!" << std::endl;
            }
        });
        
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    });
}

void stop_frontend_grpc_control_server()
{
    std::cout << "[FrontendNode] stop_frontend_grpc_control_server called" << std::endl;
    if (g_grpc_server)
    {
        std::cout << "[FrontendNode] Shutting down gRPC control server..." << std::endl;
        g_grpc_server->Shutdown();
        if (g_grpc_thread.joinable())
        {
            std::cout << "[FrontendNode] Joining gRPC thread..." << std::endl;
            g_grpc_thread.join();
        }
        std::cout << "[FrontendNode] gRPC control server stopped" << std::endl;
    }
}

bool should_frontend_accept_connections()
{
    return g_server_should_run.load();
}