# Jump Server Build and Packaging

This script automates the process of building, packaging, and distributing the Jump Server application. It compiles the `jump-server` Java application, creates a custom Java runtime, and bundles the application along with its dependencies into a distributable tarball.

## Prerequisites

Before running the `build.sh` script, make sure you have the following installed:

1. **Java Development Kit (JDK) 21+**: Ensure that JDK 21 or a compatible version is installed.
2. **Maven**: This script uses Maven to build the Java application.
3. **jdeps**: The `jdeps` tool is required for calculating the Java modules.
4. **jlink**: To create a custom Java runtime.
5. **tar**: For creating the distributable `.tar.gz` archive.

## Build Steps

### 1. Clone the Repository

First, clone the repository where the Jump Server code resides (if not already cloned):

```bash
git clone <repository-url>
cd <repository-directory>
```

### 2. Run the Build Script

The script build.sh is used to automate the entire process.
```
./build.sh
```

### 3. Verify the Output

After the script completes, the packaged bundle will be available in the builds/ directory as a .tar.gz file
```
ls -lh builds/
```

# 🛰️ BinaryFlux Jump Server — Installation Guide

This guide explains how to install, configure, and manage the BinaryFlux Jump Server on a Linux system.

---

## ✅ Requirements

- Linux server with systemd (Ubuntu 20+, RHEL 8+, CentOS 7+, etc.)
- You have received a tar.gz bundle from BinaryFlux
- No internet access required

---

## 📦 Installation Steps

1. Extract the bundle:

   ```bash
   tar -xzf jump-server-bundle-*.tar.gz
    ```
2. Place the certs folder provided by Binaryflux into this extracted jump-server-bundle
3. Review and edit the config.yml:

```yaml
tls_profile:
  client_cert: certs/client-cert.pem
  client_key: certs/client-key.pem
  ca_cert: certs/ca.pem
  server_name: <SERVER_NAME>

# Batching configuration (optional)
batch_config:
  max_batch_size: 1000        # Maximum number of records per batch
  max_batch_bytes: 1048576    # Maximum bytes per batch (1MB)
  flush_interval_ms: 1000     # Flush interval in milliseconds
  enable_compression: true    # Enable GZIP compression

routes:
  - name: windows
    listen:
      ip: "0.0.0.0"
      port: 9999
      protocol: tcp
    forward:
      host: <BACKEND_SERVICE_IP_OR_HOST>
      port: <BACKEND_SERVICE_PORT>
      tls: true         # Optional; default is true
  - name: linux
    listen:
      ip: "0.0.0.0"
      port: 9998
      protocol: tcp
    forward:
      host: <BACKEND_SERVICE_IP_OR_HOST>
      port: <BACKEND_SERVICE_PORT>
      tls: true
```

## 🔄 Batching and Compression Features

The Jump Server now supports advanced batching and compression features to optimize network performance:

### Batch Configuration Options

- **`max_batch_size`**: Maximum number of records to accumulate before sending (default: 1000)
- **`max_batch_bytes`**: Maximum total bytes to accumulate before sending (default: 1MB)
- **`flush_interval_ms`**: Time interval in milliseconds to flush batches (default: 1000ms)
- **`enable_compression`**: Enable GZIP compression for batched data (default: true)

### How Batching Works

1. **Record Accumulation**: Incoming data is accumulated in memory until batch limits are reached
2. **Compression**: When enabled, batched data is compressed using GZIP before transmission
3. **Scheduled Flushing**: Batches are automatically flushed at regular intervals
4. **Size-based Flushing**: Batches are immediately flushed when size limits are exceeded

### Benefits

- **Reduced Network Overhead**: Fewer TCP connections and headers
- **Improved Throughput**: Compression reduces bandwidth usage
- **Better Latency Control**: Configurable flush intervals balance latency vs. efficiency
- **Memory Efficiency**: Automatic memory management with configurable limits

✏️ You can define one or more input ports under sources, and one or more backend destinations under destinations.

4. Run the installer:
```
sudo ./install.sh
```

## 🛠 Managing the Service

- To check status:
```
sudo systemctl status binaryflux-jumpserver
```

- To restart (after editing config.yml):
```
sudo systemctl restart binaryflux-jumpserver
```

- To stop:
```
sudo systemctl stop binaryflux-jumpserver
```

- To view logs:
```
journalctl -u binaryflux-jumpserver -f
```

## ❌ Uninstalling the Jump Server

- To remove the jump server completely:
```
sudo ./uninstall.sh
```

This will:
 - Stop and disable the systemd service
 - Remove the service definition
 - Delete the installation directory /opt/jump

