resource "docker_network" "network" {
  name   = "kafkadev_${random_id.deployment_id.hex}_network"
  driver = "bridge"

  ipam_config {
    subnet  = var.network_cidr
    gateway = cidrhost(var.network_cidr, var.kafka_count + 2)
  }
}
