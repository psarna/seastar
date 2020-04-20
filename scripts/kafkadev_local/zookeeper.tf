data "docker_registry_image" "zookeeper" {
  name = "wurstmeister/zookeeper:latest"
}

resource "docker_image" "zookeeper" {
  name          = "${data.docker_registry_image.zookeeper.name}"
  pull_triggers = [data.docker_registry_image.zookeeper.sha256_digest]
  keep_locally  = true
}

resource "docker_container" "zookeeper" {
  name  = "kafkadev_${random_id.deployment_id.hex}_zookeeper"
  image = docker_image.zookeeper.latest

  networks_advanced {
    name         = docker_network.network.name
    ipv4_address = cidrhost(var.network_cidr, var.kafka_count + 1)
  }
}

output "zookeeper_addr" {
  value       = docker_container.zookeeper.network_data[0].ip_address
  description = "The IP address of Zookeeper."
}

output "zookeeper_name" {
  value       = docker_container.zookeeper.name
  description = "The name of Zookeeper Docker containers."
}
