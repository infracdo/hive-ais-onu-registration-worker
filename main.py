"""
Kafka ONU Registration Worker

Listens to 'olt_ont_registration' topic and automatically provisions ONUs
through the device provisioning API or directly via SNMP/Telnet.
"""
import os
import json
import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional

import requests
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv
from pysnmp.hlapi import *
import paramiko

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ONURegistrationWorker:
    """Worker that processes ONT registration events from Kafka"""
    
    def __init__(self):
        # Kafka Configuration
        self.kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.kafka_input_topic = os.getenv('KAFKA_INPUT_TOPIC', 'olt_ont_registration')
        self.kafka_output_topic = os.getenv('KAFKA_OUTPUT_TOPIC', 'registered_onu')
        self.kafka_consumer_group = os.getenv('KAFKA_CONSUMER_GROUP', 'onu-registration-worker')
        
        # Configuration Method: 'api', 'snmp', or 'telnet'
        self.config_method = os.getenv('CONFIG_METHOD', 'api').lower()
        
        # API Configuration
        self.api_base_url = os.getenv('API_BASE_URL', 'http://localhost:8000')
        
        # SNMP Configuration
        self.snmp_community = os.getenv('SNMP_COMMUNITY', 'devCommunity')
        self.snmp_version = os.getenv('SNMP_VERSION', '2c')
        
        # Telnet Configuration  
        self.telnet_username = os.getenv('TELNET_USERNAME', 'xgate')
        self.telnet_password = os.getenv('TELNET_PASSWORD', '')
        self.telnet_port = int(os.getenv('TELNET_PORT', '23'))
        
        # Gotify Configuration
        self.gotify_url = os.getenv('GOTIFY_URL', 'https://gotify.mcandres.com')
        self.gotify_token = os.getenv('GOTIFY_TOKEN', 'AfvvshNxqj6wk59')
        
        # ONU Default Configuration
        self.dhcp_enable = os.getenv('ONU_DHCP_ENABLE', 'true').lower() == 'true'
        self.gemport = int(os.getenv('ONU_GEMPORT', '1'))
        self.iphost = int(os.getenv('ONU_IPHOST', '1'))
        self.mode = os.getenv('ONU_MODE', 'tag')
        self.onu_type = os.getenv('ONU_TYPE', 'ZTE-F622')
        self.ping_response = os.getenv('ONU_PING_RESPONSE', 'true').lower() == 'true'
        self.service_port = int(os.getenv('ONU_SERVICE_PORT', '1'))
        self.switchport_bind = os.getenv('ONU_SWITCHPORT_BIND', 'switch_0/1')
        self.tcont = int(os.getenv('ONU_TCONT', '1'))
        self.tcont_profile = os.getenv('ONU_TCONT_PROFILE', '10M')
        self.traceroute_response = os.getenv('ONU_TRACEROUTE_RESPONSE', 'true').lower() == 'true'
        self.user_vlan = int(os.getenv('ONU_USER_VLAN', '100'))
        self.vlan = int(os.getenv('ONU_VLAN', '100'))
        self.vlan_port = os.getenv('ONU_VLAN_PORT', 'eth_0/1')
        self.vport = int(os.getenv('ONU_VPORT', '1'))
        
        # Initialize Kafka consumer
        self.consumer = None
        self.producer = None
        
    def connect_kafka(self):
        """Initialize Kafka consumer and producer"""
        try:
            logger.info(f"Connecting to Kafka: {self.kafka_bootstrap_servers}")
            
            # Create consumer
            self.consumer = KafkaConsumer(
                self.kafka_input_topic,
                bootstrap_servers=self.kafka_bootstrap_servers,
                group_id=self.kafka_consumer_group,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                enable_auto_commit=True,
                consumer_timeout_ms=1000
            )
            
            # Create producer
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            
            logger.info(f"✅ Connected to Kafka")
            logger.info(f"📥 Listening to topic: {self.kafka_input_topic}")
            logger.info(f"📤 Publishing to topic: {self.kafka_output_topic}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise
    
    def send_gotify_notification(self, title: str, message: str, priority: int = 5, extras: Optional[Dict] = None):
        """Send notification to Gotify"""
        try:
            url = f"{self.gotify_url}/message?token={self.gotify_token}"
            
            payload = {
                "title": title,
                "message": message,
                "priority": priority
            }
            
            if extras:
                payload["extras"] = extras
            
            response = requests.post(url, json=payload, timeout=5)
            response.raise_for_status()
            logger.debug(f"📱 Gotify notification sent: {title}")
            
        except Exception as e:
            logger.warning(f"Failed to send Gotify notification: {e}")
    
    def get_device_by_ip(self, source_ip: str) -> Optional[Dict[str, Any]]:
        """Get device information by IP address"""
        try:
            url = f"{self.api_base_url}/api/v1/devices/by-ip/{source_ip}"
            logger.info(f"🔍 Getting device info for IP: {source_ip}")
            
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            device = response.json()
            logger.info(f"✅ Found device: {device['name']} (ID: {device['id']})")
            return device
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Failed to get device info for {source_ip}: {e}")
            return None
    
    def get_device_by_id(self, device_id: int) -> Optional[Dict[str, Any]]:
        """Get device information by device ID"""
        try:
            url = f"{self.api_base_url}/api/v1/devices/{device_id}"
            logger.info(f"🔍 Getting device info for ID: {device_id}")
            
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            device = response.json()
            logger.info(f"✅ Found device: {device['name']} (ID: {device['id']})")
            return device
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Failed to get device info for ID {device_id}: {e}")
            return None
    
    def get_pppoe_user_by_onu_serial(self, ont_serial: str) -> Optional[Dict[str, Any]]:
        """Get PPPoE user by ONU serial number"""
        try:
            url = f"{self.api_base_url}/api/v1/pppoe/users/by-onu-serial-number/{ont_serial}"
            logger.info(f"🔍 Getting PPPoE user for ONU serial: {ont_serial}")
            
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            user = response.json()
            logger.info(f"✅ Found PPPoE user: {user['user_name']} (ID: {user['id']})")
            return user
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Failed to get PPPoE user for ONU serial {ont_serial}: {e}")
            return None
    
    def register_onu(self, device_id: int, ont_serial: str) -> Optional[Dict[str, Any]]:
        """Register ONU through the API"""
        try:
            url = f"{self.api_base_url}/api/v1/olt/onu/register"
            
            payload = {
                "description": f"Customer Fiber connection for {ont_serial}",
                "device_id": device_id,
                "dhcp_enable": self.dhcp_enable,
                "gemport": self.gemport,
                "iphost": self.iphost,
                "mode": self.mode,
                "name": f"ONU-Customer-{ont_serial}",
                "onu_serial_number": ont_serial,
                "onu_type": self.onu_type,
                "ping_response": self.ping_response,
                "service_port": self.service_port,
                "switchport_bind": self.switchport_bind,
                "tcont": self.tcont,
                "tcont_profile": self.tcont_profile,
                "traceroute_response": self.traceroute_response,
                "user_vlan": self.user_vlan,
                "vlan": self.vlan,
                "vlan_port": self.vlan_port,
                "vport": self.vport
            }
            
            logger.info(f"📝 Registering ONU: {ont_serial} on device ID: {device_id}")
            logger.debug(f"Payload: {json.dumps(payload, indent=2)}")
            
            response = requests.post(url, json=payload, timeout=30)
            response.raise_for_status()
            
            result = response.json()
            logger.info(f"✅ ONU registered successfully: {ont_serial}")
            return result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Failed to register ONU {ont_serial}: {e}")
            if hasattr(e.response, 'text'):
                logger.error(f"Response: {e.response.text}")
            return None
    
    def configure_onu_via_snmp(self, device_host: str, ont_serial: str, pon_port: str = "1/1/1", onu_id: int = None) -> Optional[Dict[str, Any]]:
        """
        Configure ONU directly via SNMP
        Note: This is a placeholder as ZTE SNMP OIDs for ONU configuration need to be discovered
        """
        try:
            logger.info(f"🔧 Configuring ONU via SNMP: {ont_serial} on {device_host}")
            logger.warning("⚠️  SNMP ONU configuration not yet implemented - OIDs need to be discovered")
            logger.info("   Use CONFIG_METHOD='api' or 'telnet' for now")
            logger.info("   See snmp-walker-app for OID discovery")
            
            # TODO: Implement SNMP configuration once OIDs are discovered
            # Key OIDs needed (to be discovered):
            # - ONU registration OID
            # - T-CONT profile assignment OID
            # - GEM port configuration OID
            # - Service port configuration OID
            # - VLAN configuration OID
            #
            # Example structure (once OIDs are known):
            # 1. Register ONU serial number
            # 2. Assign T-CONT profile
            # 3. Create GEM ports
            # 4. Configure service ports
            # 5. Set VLAN configuration
            
            return {
                "status": "not_implemented",
                "message": "SNMP configuration requires OID discovery",
                "method": "snmp",
                "serial_number": ont_serial
            }
            
        except Exception as e:
            logger.error(f"❌ SNMP configuration failed: {e}")
            return None
    
    def configure_onu_via_telnet(self, device_host: str, ont_serial: str, 
                                  pon_port: str = "1/1/1", onu_id: int = None) -> Optional[Dict[str, Any]]:
        """Configure ONU directly via Telnet"""
        try:
            logger.info(f"🔧 Configuring ONU via Telnet: {ont_serial} on {device_host}")
            
            # Connect to device
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            logger.info(f"Connecting to {device_host}:{self.telnet_port}")
            ssh.connect(
                device_host,
                port=self.telnet_port,
                username=self.telnet_username,
                password=self.telnet_password,
                timeout=10,
                look_for_keys=False,
                allow_agent=False
            )
            
            # Get shell
            shell = ssh.invoke_shell()
            time.sleep(1)
            
            # Clear initial output
            shell.recv(9999)
            
            # Build configuration commands
            commands = [
                "configure terminal",
                f"interface gpon-onu_{pon_port}:{onu_id}",
                f"  name ONU-Customer-{ont_serial}",
                f"  description Customer Fiber connection for {ont_serial}",
                "  sn-bind enable sn",
                f"  tcont {self.tcont} profile {self.tcont_profile}",
                f"  gemport {self.gemport} tcont {self.tcont}",
                f"  service-port {self.service_port} vport {self.vport} user-vlan {self.user_vlan} vlan {self.vlan}",
                "  exit",
                f"pon-onu-mng gpon-onu_{pon_port}:{onu_id}",
                f"  service {self.service_port} gemport {self.gemport} vlan {self.vlan}",
                f"  switchport-bind {self.switchport_bind} iphost {self.iphost}",
                f"  ip-host {self.iphost} dhcp-enable {'enable' if self.dhcp_enable else 'disable'} ping-response {'enable' if self.ping_response else 'disable'} traceroute-response {'enable' if self.traceroute_response else 'disable'}",
                f"  vlan port {self.vlan_port} mode {self.mode} vlan {self.vlan}",
                "  exit",
                "exit",
                "write"
            ]
            
            # Execute commands
            for cmd in commands:
                logger.debug(f"Executing: {cmd}")
                shell.send(cmd + "\n")
                time.sleep(0.5)
            
            # Get final output
            time.sleep(2)
            output = shell.recv(9999).decode('utf-8', errors='ignore')
            logger.debug(f"Telnet output: {output}")
            
            ssh.close()
            
            logger.info(f"✅ ONU configured via Telnet: {ont_serial}")
            
            return {
                "status": "success",
                "message": "ONU configured via Telnet",
                "method": "telnet",
                "serial_number": ont_serial,
                "pon_port": pon_port,
                "onu_id": onu_id,
                "commands_executed": len(commands)
            }
            
        except Exception as e:
            logger.error(f"❌ Telnet configuration failed: {e}")
            return None
    
    def publish_result(self, original_message: Dict[str, Any], 
                      registration_result: Dict[str, Any],
                      device_info: Dict[str, Any]):
        """Publish registration result to output topic"""
        try:
            output_message = {
                "processed_at": datetime.utcnow().isoformat() + 'Z',
                "status": "success",
                "original_trap": original_message,
                "device_info": {
                    "id": device_info['id'],
                    "name": device_info['name'],
                    "manufacturer": device_info['manufacturer'],
                    "host": device_info['host']
                },
                "registration_result": registration_result
            }
            
            self.producer.send(self.kafka_output_topic, value=output_message)
            self.producer.flush()
            
            logger.info(f"📤 Published result to {self.kafka_output_topic}")
            
        except Exception as e:
            logger.error(f"❌ Failed to publish result: {e}")
    
    def publish_error(self, original_message: Dict[str, Any], error_message: str):
        """Publish error message to output topic"""
        try:
            output_message = {
                "processed_at": datetime.utcnow().isoformat() + 'Z',
                "status": "error",
                "error": error_message,
                "original_trap": original_message
            }
            
            self.producer.send(self.kafka_output_topic, value=output_message)
            self.producer.flush()
            
            logger.warning(f"📤 Published error to {self.kafka_output_topic}")
            
        except Exception as e:
            logger.error(f"❌ Failed to publish error: {e}")
    
    def process_message(self, message: Dict[str, Any]):
        """Process a single ONT registration message"""
        try:
            logger.info("=" * 80)
            logger.info("📨 New ONT Registration Event")
            logger.info("=" * 80)
            
            # Extract data from message
            source_ip = message.get('source_ip')
            ont_data = message.get('ont_data', {})
            ont_serial = ont_data.get('ont_serial')
            ont_model = ont_data.get('ont_model')
            ont_firmware = ont_data.get('ont_firmware')
            registration_time = ont_data.get('registration_time')
            vendor_id = ont_data.get('vendor_id')
            
            if not source_ip or not ont_serial:
                error_msg = "Missing required fields: source_ip or ont_serial"
                logger.error(f"❌ {error_msg}")
                self.publish_error(message, error_msg)
                return
            
            logger.info(f"🔢 Serial Number: {ont_serial}")
            logger.info(f"📡 Model: {ont_model}")
            logger.info(f"📍 Source IP: {source_ip}")
            logger.info(f"⏰ Registration Time: {registration_time}")
            
            # Send notification: ONT registration received
            self.send_gotify_notification(
                title="🆕 ONT Registration Detected",
                message=f"Serial: {ont_serial}\n"
                        f"Model: {ont_model}\n"
                        f"Vendor: {vendor_id}\n"
                        f"Firmware: {ont_firmware}\n"
                        f"Source OLT: {source_ip}\n"
                        f"Time: {registration_time}",
                priority=5,
                extras={
                    "ontSerial": ont_serial,
                    "ontModel": ont_model,
                    "sourceIp": source_ip
                }
            )
            
            # Step 1: Get device information by IP
            device_info = self.get_device_by_ip(source_ip)
            
            # Step 1b: If device not found by IP, try to get it from PPPoE user's onu_olt_deviceid
            if not device_info:
                logger.warning(f"⚠️  Device not found by IP: {source_ip}, trying PPPoE user lookup...")
                pppoe_user = self.get_pppoe_user_by_onu_serial(ont_serial)
                
                if pppoe_user and pppoe_user.get('onu_olt_deviceid'):
                    device_id = pppoe_user['onu_olt_deviceid']
                    logger.info(f"📋 Found device ID from PPPoE user: {device_id}")
                    device_info = self.get_device_by_id(device_id)
                    
                    if device_info:
                        logger.info(f"✅ Device found via PPPoE user: {device_info['name']}")
                    else:
                        error_msg = f"Device ID {device_id} from PPPoE user not found"
                        logger.error(f"❌ {error_msg}")
                        self.publish_error(message, error_msg)
                        
                        # Send failure notification
                        self.send_gotify_notification(
                            title="❌ ONT Registration Failed",
                            message=f"Serial: {ont_serial}\n"
                                    f"Model: {ont_model}\n"
                                    f"PPPoE User: {pppoe_user['user_name']}\n"
                                    f"Error: {error_msg}",
                            priority=8,
                            extras={
                                "ontSerial": ont_serial,
                                "error": error_msg,
                                "status": "failed"
                            }
                        )
                        return
                else:
                    error_msg = f"No device found for IP {source_ip} and no onu_olt_deviceid in PPPoE user for ONU serial {ont_serial}"
                    logger.error(f"❌ {error_msg}")
                    self.publish_error(message, error_msg)
                    
                    # Send failure notification
                    self.send_gotify_notification(
                        title="❌ ONT Registration Failed",
                        message=f"Serial: {ont_serial}\n"
                                f"Model: {ont_model}\n"
                                f"Source IP: {source_ip}\n"
                                f"Error: {error_msg}",
                        priority=8,
                        extras={
                            "ontSerial": ont_serial,
                            "error": error_msg,
                            "status": "failed"
                        }
                    )
                    return
            
            device_id = device_info['id']
            device_name = device_info['name']
            device_host = device_info['host']
            
            # Step 2: Register/Configure ONU based on method
            logger.info(f"📋 Configuration Method: {self.config_method.upper()}")
            
            registration_result = None
            
            if self.config_method == 'api':
                # Use FastAPI endpoint
                registration_result = self.register_onu(device_id, ont_serial)
                
            elif self.config_method == 'snmp':
                # Direct SNMP configuration
                logger.warning("⚠️  SNMP method selected but not yet fully implemented")
                logger.info("   Falling back to API method")
                logger.info("   To implement SNMP: discover OIDs using snmp-walker-app")
                registration_result = self.register_onu(device_id, ont_serial)
                # TODO: Uncomment when SNMP is implemented
                # registration_result = self.configure_onu_via_snmp(device_host, ont_serial)
                
            elif self.config_method == 'telnet':
                # Direct Telnet configuration
                # Note: Requires PON port and ONU ID from trap or configuration
                pon_port = ont_data.get('pon_port', '1/1/1')
                onu_id = ont_data.get('onu_id')
                
                if not onu_id:
                    logger.warning("⚠️  ONU ID not found in trap data, using API method")
                    registration_result = self.register_onu(device_id, ont_serial)
                else:
                    registration_result = self.configure_onu_via_telnet(
                        device_host, ont_serial, pon_port, onu_id
                    )
            else:
                error_msg = f"Unknown configuration method: {self.config_method}"
                logger.error(f"❌ {error_msg}")
                self.publish_error(message, error_msg)
                return
            
            if not registration_result:
                error_msg = f"Failed to register ONU: {ont_serial}"
                logger.error(f"❌ {error_msg}")
                self.publish_error(message, error_msg)
                
                # Send failure notification
                self.send_gotify_notification(
                    title="❌ ONT Registration Failed",
                    message=f"Serial: {ont_serial}\n"
                            f"Model: {ont_model}\n"
                            f"Device: {device_name} ({source_ip})\n"
                            f"Error: Failed to register ONU through API",
                    priority=8,
                    extras={
                        "ontSerial": ont_serial,
                        "deviceName": device_name,
                        "error": error_msg,
                        "status": "failed"
                    }
                )
                return
            
            # Step 3: Publish success result
            self.publish_result(message, registration_result, device_info)
            
            # Send success notification
            self.send_gotify_notification(
                title="✅ ONT Registration Successful",
                message=f"Serial: {ont_serial}\n"
                        f"Model: {ont_model}\n"
                        f"Device: {device_name} ({source_ip})\n"
                        f"VLAN: {self.vlan}\n"
                        f"Profile: {self.tcont_profile}\n"
                        f"Status: Successfully provisioned",
                priority=5,
                extras={
                    "ontSerial": ont_serial,
                    "ontModel": ont_model,
                    "deviceName": device_name,
                    "vlan": self.vlan,
                    "status": "success"
                }
            )
            
            logger.info("=" * 80)
            logger.info("✅ ONT Registration Process Completed Successfully")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"❌ Error processing message: {e}", exc_info=True)
            self.publish_error(message, str(e))
    
    def run(self):
        """Main worker loop"""
        logger.info("🚀 Starting ONU Registration Worker")
        logger.info(f"API Base URL: {self.api_base_url}")
        logger.info(f"Default ONU Type: {self.onu_type}")
        logger.info(f"Default VLAN: {self.vlan}")
        logger.info(f"Default TCONT Profile: {self.tcont_profile}")
        
        # Connect to Kafka
        self.connect_kafka()
        
        logger.info("👂 Waiting for ONT registration events...")
        logger.info("Press Ctrl+C to stop")
        
        try:
            while True:
                try:
                    # Poll for messages
                    for message in self.consumer:
                        self.process_message(message.value)
                    
                    # Small sleep to prevent busy waiting
                    time.sleep(0.1)
                    
                except Exception as e:
                    logger.error(f"Error in consumer loop: {e}", exc_info=True)
                    time.sleep(5)  # Wait before retrying
                    
        except KeyboardInterrupt:
            logger.info("⏹️  Shutting down worker...")
        finally:
            if self.consumer:
                self.consumer.close()
            if self.producer:
                self.producer.close()
            logger.info("👋 Worker stopped")


if __name__ == "__main__":
    worker = ONURegistrationWorker()
    worker.run()
