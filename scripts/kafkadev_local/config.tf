provider "docker" {
  host = "unix:///var/run/docker.sock"
}

variable "network_cidr" {
  type        = string
  description = "The IPv4 network prefix for started containers, written in CIDR format, e.g. 172.13.0.0/16."
}

variable "kafka_count" {
  type        = number
  description = "The number of started Kafka brokers."
}

resource "random_id" "deployment_id" {
  byte_length = 2
}
