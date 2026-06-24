# ONU Registration Worker with Multi-Method Configuration

Automatically provisions ONUs detected via SNMP traps. Supports three configuration methods: API, SNMP, and Telnet.

## Overview

This worker listens to `olt_ont_registration` Kafka topic for ONU registration events and automatically provisions them using your chosen method.

## Configuration Methods

### Method 1: API (Recommended) ✅
- **When to use**: Production environments, need validation and logging
- **Pros**: Validated, logged, consistent with other operations
- **Cons**: Requires FastAPI to be running
- **Status**: Fully implemented and tested

### Method 2: SNMP 🚧
- **When to use**: Direct device configuration, API-independent operation  
- **Pros**: No API dependency, potentially faster
- **Cons**: Requires OID discovery (see snmp-walker-app)
- **Status**: Placeholder - needs OID mapping implementation

### Method 3: Telnet 🔧
- **When to use**: Fallback when API unavailable
- **Pros**: Direct device access, no API needed
- **Cons**: Less secure, requires ONU ID in trap data
- **Status**: Implemented but requires ONU ID

## Features

✅ Three configuration methods (API/SNMP/Telnet)
✅ Automatic device lookup by IP
✅ Gotify notifications  
✅ Kafka result publishing
✅ Configurable ONU defaults
✅ Docker deployment ready

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
cp .env.example .env
# Edit .env with your settings
```

**Key Configuration:**
```bash
# Choose method
CONFIG_METHOD=api   # or 'snmp' or 'telnet'

# API method (default)
API_BASE_URL=http://localhost:8000

# SNMP method (when implemented)
SNMP_COMMUNITY=devCommunity

# Telnet method
TELNET_USERNAME=xgate
TELNET_PASSWORD=yourpassword
```

### 3. Run Worker

```bash
# Load environment
source load_env.sh

# Run worker
python3 main.py
```

## Configuration Method Details

### Using API Method (Default)

```bash
CONFIG_METHOD=api
API_BASE_URL=http://localhost:8000
```

**Flow:**
1. Receive trap from Kafka
2. Query API for device info by IP
3. Call API endpoint `/api/v1/olt/onu/register`
4. Publish result to Kafka

**Advantages:**
- Full validation
- Database tracking
- Consistent logging
- Error handling

### Using SNMP Method (Future)

```bash
CONFIG_METHOD=snmp
SNMP_COMMUNITY=devCommunity
```

**Status**: 🚧 Not yet implemented

**Requirements:**
1. Complete OID discovery using `snmp-walker-app`
2. Map OIDs to configuration parameters:
   - ONU registration OID
   - T-CONT assignment OID
   - GEM port creation OID
   - Service port configuration OID
   - VLAN assignment OID
3. Implement SNMP SET operations
4. Test on device

**Next Steps:**
See `/snmp-walker-app/` for OID discovery tools and guides.

### Using Telnet Method

```bash
CONFIG_METHOD=telnet
TELNET_USERNAME=xgate
TELNET_PASSWORD=yourpassword
TELNET_PORT=23
```

**Requirements:**
- ONU ID must be included in trap data
- PON port information (defaults to 1/1/1)

**Flow:**
1. Receive trap from Kafka
2. Extract ONU ID and PON port from trap
3. SSH/Telnet to device
4. Execute configuration commands directly
5. Publish result to Kafka

**Configuration Commands Executed:**
```
configure terminal
interface gpon-onu_1/1/1:1
  name ONU-Customer-SERIAL
  description Customer Fiber connection
  sn-bind enable sn
  tcont 1 profile 10M
  gemport 1 tcont 1
  service-port 1 vport 1 user-vlan 100 vlan 100
  exit
pon-onu-mng gpon-onu_1/1/1:1
  service 1 gemport 1 vlan 100
  switchport-bind switch_0/1 iphost 1
  ip-host 1 dhcp-enable enable ping-response enable
  vlan port eth_0/1 mode tag vlan 100
  exit
exit
write
```

## Environment Variables

### Kafka Configuration
| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `10.42.4.19:9092` | Kafka broker address |
| `KAFKA_INPUT_TOPIC` | `olt_ont_registration` | Input topic for traps |
| `KAFKA_OUTPUT_TOPIC` | `registered_onu` | Output topic for results |
| `KAFKA_CONSUMER_GROUP` | `onu-registration-worker` | Consumer group ID |

### Method Configuration
| Variable | Default | Description |
|----------|---------|-------------|
| `CONFIG_METHOD` | `api` | Configuration method: api/snmp/telnet |

### API Configuration (CONFIG_METHOD=api)
| Variable | Default | Description |
|----------|---------|-------------|
| `API_BASE_URL` | `http://localhost:8000` | FastAPI base URL |

### SNMP Configuration (CONFIG_METHOD=snmp)
| Variable | Default | Description |
|----------|---------|-------------|
| `SNMP_COMMUNITY` | `devCommunity` | SNMP read-write community |
| `SNMP_VERSION` | `2c` | SNMP version |

### Telnet Configuration (CONFIG_METHOD=telnet)
| Variable | Default | Description |
|----------|---------|-------------|
| `TELNET_USERNAME` | `xgate` | Device SSH/Telnet username |
| `TELNET_PASSWORD` | `` | Device SSH/Telnet password |
| `TELNET_PORT` | `23` | Telnet port (22 for SSH) |

### ONU Defaults
| Variable | Default | Description |
|----------|---------|-------------|
| `ONU_TYPE` | `ZTE-F622` | ONU model type |
| `ONU_TCONT` | `1` | T-CONT number |
| `ONU_TCONT_PROFILE` | `10M` | T-CONT profile name |
| `ONU_GEMPORT` | `1` | GEM port number |
| `ONU_SERVICE_PORT` | `1` | Service port number |
| `ONU_VPORT` | `1` | Virtual port number |
| `ONU_VLAN` | `100` | VLAN ID |
| `ONU_USER_VLAN` | `100` | User VLAN ID |
| `ONU_MODE` | `tag` | VLAN mode (tag/untag) |
| `ONU_VLAN_PORT` | `eth_0/1` | Ethernet port |
| `ONU_SWITCHPORT_BIND` | `switch_0/1` | Switch port binding |
| `ONU_IPHOST` | `1` | IP host number |
| `ONU_DHCP_ENABLE` | `true` | Enable DHCP |
| `ONU_PING_RESPONSE` | `true` | Enable ping response |
| `ONU_TRACEROUTE_RESPONSE` | `true` | Enable traceroute |

## Docker Deployment

### Build and Push

```bash
./docker-build-push.sh
```

### Run with Docker Compose

```bash
docker-compose up -d
```

### View Logs

```bash
docker-compose logs -f onu-registration-worker
```

## Input Message Format

```json
{
  "timestamp": "2025-11-03T06:25:19.238160Z",
  "source_ip": "10.42.3.24",
  "event_type": "ont_registration",
  "ont_data": {
    "event_type": "ont_registration",
    "ont_model": "F612WV6.0",
    "ont_serial": "RLGMFE1CB160",
    "ont_firmware": "V2.1.5-47907",
    "registration_time": "2025-11-03 14:25:18",
    "ont_index": "285278465",
    "ont_password": null,
    "vendor_id": "RLGM",
    "device_serial": "FE1CB160"
  },
  "trap_oid": "1.3.6.1.4.1.3902.1082.500.10.3.1.80"
}
```

## Output Message Format

```json
{
  "processed_at": "2025-11-03T06:26:00.000000Z",
  "status": "success",
  "config_method": "api",
  "original_trap": { /* original message */ },
  "device_info": {
    "id": 9,
    "name": "ZTE OLT",
    "manufacturer": "zte",
    "host": "10.42.3.24"
  },
  "registration_result": {
    "status": "success",
    "message": "ONU registered successfully",
    "onu_serial": "RLGMFE1CB160"
  }
}
```

## Implementing SNMP Configuration

To enable SNMP configuration method:

### Step 1: Discover OIDs
```bash
cd ../snmp-walker-app
python3 zte_oid_walker.py --ip 10.42.3.24 --community devCommunity
```

### Step 2: Map OIDs to Operations
Document OIDs for:
- ONU registration
- T-CONT assignment  
- GEM port creation
- Service port configuration
- VLAN configuration

### Step 3: Implement in Code
Update `configure_onu_via_snmp()` method in `main.py`:
```python
def configure_onu_via_snmp(self, device_host: str, ont_serial: str, ...):
    # Set ONU serial OID
    snmpset(..., ONU_SERIAL_OID, ont_serial)
    
    # Set T-CONT profile OID
    snmpset(..., TCONT_PROFILE_OID, self.tcont_profile)
    
    # etc...
```

### Step 4: Test
```bash
CONFIG_METHOD=snmp python3 main.py
```

## Troubleshooting

### SNMP Method Shows "Not Implemented"
- This is expected - SNMP OIDs need to be discovered first
- See `snmp-walker-app/` for OID discovery tools
- Use API or Telnet method in the meantime

### Telnet Method Fails
- Check if ONU ID is in trap data
- Verify Telnet credentials
- Ensure device is accessible
- Check firewall rules for port 23

### API Method Connection Refused
- Ensure FastAPI is running
- Check `API_BASE_URL` setting
- Verify network connectivity
- Check API logs for errors

## Related Components

- **FastAPI App**: Main provisioning API
- **SNMP Walker**: OID discovery tool for SNMP implementation
- **SNMP Trap Receiver**: Generates registration events

## License

Part of Apollo Device Provisioner system.
