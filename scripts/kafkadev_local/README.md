# Kafkadev (local)

Dev/test local deployment of Kafka (and Zookeeper) using Docker.

## Dependencies
- Docker
- Terraform v0.12

### Installing Docker
<pre>
# Install and start Docker
<b>
$ sudo dnf install docker
$ sudo systemctl start docker
</b>
# Add current user to docker group
<b>
$ sudo groupadd docker
$ sudo usermod -aG docker $USER
$ newgrp docker 
</b></pre>

### Installing Terraform
<pre><b>$ wget https://releases.hashicorp.com/terraform/0.12.12/terraform_0.12.12_linux_amd64.zip
$ unzip terraform_0.12.12_linux_amd64.zip
$ sudo cp terraform /usr/bin/
</b></pre>

## Usage
### Starting cluster
The following example creates a Kafka cluster with 5 brokers and a configured Zookeeper instance in the `172.13.0.0/16` network. 

The IP addresses of started Kafka brokers are printed as `kafka_addr` and the names of Docker containers are exported as `kafka_name`.

Run the commands in the `kafkadev_local` directory:

<pre>
<b>$ terraform init</b>
<b>$ terraform apply</b>

var.kafka_count
  The number of started Kafka brokers.

  Enter a value: <b>5</b>

var.network_cidr
  The IPv4 network prefix for started containers, written in CIDR format, e.g. 172.13.0.0/16.

  Enter a value: <b>172.13.0.0/16</b>

[...]

Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value: <b>yes</b>

[...]

Apply complete! Resources: 10 added, 0 changed, 0 destroyed.

Outputs:
<b>
kafka_addr = [
  "172.13.0.1",
  "172.13.0.2",
  "172.13.0.3",
  "172.13.0.4",
  "172.13.0.5",
]
kafka_name = [
  "kafkadev_6b34_kafka1",
  "kafkadev_6b34_kafka2",
  "kafkadev_6b34_kafka3",
  "kafkadev_6b34_kafka4",
  "kafkadev_6b34_kafka5",
]</b>
zookeeper_addr = 172.13.0.6
zookeeper_name = kafkadev_6b34_zookeeper
</pre>
### Stopping cluster
Use the following command to stop and remove the Kafka cluster:

<pre>
<b>$ terraform destroy</b>
[...]

Do you really want to destroy all resources?
  Terraform will destroy all your managed infrastructure, as shown above.
  There is no undo. Only 'yes' will be accepted to confirm.

  Enter a value: <b>yes</b>
</pre>
