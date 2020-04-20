data "docker_registry_image" "kafka" {
  name = "wurstmeister/kafka:latest"
}

resource "docker_image" "kafka" {
  name          = "${data.docker_registry_image.kafka.name}"
  pull_triggers = [data.docker_registry_image.kafka.sha256_digest]
  keep_locally  = true
}

resource "docker_container" "kafka" {
  count = var.kafka_count

  name  = "kafkadev_${random_id.deployment_id.hex}_kafka${count.index + 1}"
  image = docker_image.kafka.latest

  env = [
    "KAFKA_ADVERTISED_PORT=9092",
    "KAFKA_ADVERTISED_HOST_NAME=${cidrhost(var.network_cidr, count.index + 1)}",
    "KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://${cidrhost(var.network_cidr, count.index + 1)}:9092",
    "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT",
    "KAFKA_ZOOKEEPER_CONNECT=${docker_container.zookeeper.network_data[0].ip_address}:2181"
  ]

  networks_advanced {
    name         = docker_network.network.name
    ipv4_address = cidrhost(var.network_cidr, count.index + 1)
  }
}

output "kafka_addr" {
  value       = docker_container.kafka[*].network_data[0].ip_address
  description = "The IP addresses of started Kafka brokers."
}

output "kafka_name" {
  value       = docker_container.kafka[*].name
  description = "The names of Kafka Docker containers."
}
