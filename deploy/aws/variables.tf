# ---------------------------------------------------------------------------
# Provider / identity
# ---------------------------------------------------------------------------

variable "aws_region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "us-east-1"
}

variable "aws_profile" {
  description = "Named AWS CLI/SDK profile used for credentials (the user runs a 'personal' profile)."
  type        = string
  default     = "personal"
}

variable "project_name" {
  description = "Project/name prefix applied to all resources and the Name tag."
  type        = string
  default     = "spark-connect"
}

variable "tags" {
  description = "Extra tags merged into the provider default_tags."
  type        = map(string)
  default     = {}
}

# ---------------------------------------------------------------------------
# Network
# ---------------------------------------------------------------------------

variable "vpc_cidr" {
  description = "CIDR block for the fresh VPC."
  type        = string
  default     = "10.42.0.0/16"
}

variable "public_subnet_cidrs" {
  description = "CIDR blocks for the public subnets (one per AZ). NAT GW + Client VPN + NLB live here logic-side; must be 2 for 2 AZs."
  type        = list(string)
  default     = ["10.42.0.0/24", "10.42.1.0/24"]
}

variable "private_subnet_cidrs" {
  description = "CIDR blocks for the private subnets (one per AZ). The EC2 Connect server runs here. Must be 2 for 2 AZs."
  type        = list(string)
  default     = ["10.42.10.0/24", "10.42.11.0/24"]
}

variable "data_volume_az_index" {
  description = <<-EOT
    Index (0-based) of the private subnet/AZ the Connect server's ASG is pinned to.
    EBS volumes are AZ-bound; pinning the single-instance ASG to one AZ guarantees the
    dedicated data volume can always re-attach after an instance replacement. See README
    "Durability".
  EOT
  type        = number
  default     = 0
}

# ---------------------------------------------------------------------------
# Compute / EC2
# ---------------------------------------------------------------------------

variable "instance_type" {
  description = "EC2 instance type for the Spark Connect server."
  type        = string
  default     = "r7i.xlarge"
}

variable "root_volume_gb" {
  description = "Size (GiB) of the gp3 root volume."
  type        = number
  default     = 30
}

variable "data_volume_gb" {
  description = "Size (GiB) of the dedicated gp3 data volume mounted at /srv/spark."
  type        = number
  default     = 100
}

variable "spark_version" {
  description = "Apache Spark version installed by user-data."
  type        = string
  default     = "4.1.1"
}

variable "spark_download_base_url" {
  description = "Base URL to fetch the Spark binary tarball from (Apache archive by default)."
  type        = string
  default     = "https://archive.apache.org/dist/spark"
}

variable "app_repo_url" {
  description = <<-EOT
    Git URL of THIS project repo. user-data clones it to obtain the server launcher +
    systemd unit authored by the separate task:
      connect/scripts/start-connect-server.sh, connect/scripts/stop-connect-server.sh,
      deploy/connect-server/spark-connect.service
  EOT
  type        = string
  default     = "https://github.com/CHANGE-ME/safe-spark-agents.git"
}

variable "app_repo_ref" {
  description = "Git ref (branch/tag/sha) of the app repo to check out on the instance."
  type        = string
  default     = "main"
}

variable "connect_grpc_port" {
  description = "gRPC port the Spark Connect server listens on."
  type        = number
  default     = 15002
}

# ---------------------------------------------------------------------------
# Access — AWS Client VPN (certificate / mutual auth)
# ---------------------------------------------------------------------------

variable "allowed_users" {
  description = "Human usernames authorized to reach the server (used for tagging/inventory + README onboarding). Default = 3 placeholders."
  type        = list(string)
  default     = ["user1", "user2", "user3"]
}

variable "vpn_client_cidr" {
  description = "CIDR pool handed out to connected VPN clients. MUST NOT overlap vpc_cidr; /22 minimum per AWS."
  type        = string
  default     = "172.16.0.0/22"
}

variable "vpn_server_cert_arn" {
  description = <<-EOT
    ACM ARN of the VPN SERVER certificate (mutual TLS). Generate with easy-rsa and
    `aws acm import-certificate` — see README "Onboard a Client VPN user". Required for apply.
  EOT
  type        = string
  default     = ""
}

variable "vpn_client_root_cert_arn" {
  description = <<-EOT
    ACM ARN of the client ROOT CA certificate chain used to validate client certs.
    May equal vpn_server_cert_arn when the same CA signs both. Required for apply.
  EOT
  type        = string
  default     = ""
}

variable "vpn_split_tunnel" {
  description = "Enable split tunnel so only VPC-bound traffic crosses the VPN."
  type        = bool
  default     = true
}

# ---------------------------------------------------------------------------
# NLB / TLS — ACM
# ---------------------------------------------------------------------------

variable "create_acm_cert" {
  description = "true = create+DNS-validate a new ACM cert for domain_name; false = use existing_acm_cert_arn."
  type        = bool
  default     = false
}

variable "domain_name" {
  description = "FQDN clients use to reach the server (e.g. spark-connect.example.com). Required when create_acm_cert = true."
  type        = string
  default     = ""
}

variable "existing_acm_cert_arn" {
  description = "ARN of a pre-existing ACM cert for the NLB TLS listener. Required when create_acm_cert = false."
  type        = string
  default     = ""
}

variable "route53_zone_id" {
  description = <<-EOT
    Optional Route53 hosted zone id. When create_acm_cert = true and this is set, Terraform
    creates the DNS validation records AND an alias/CNAME for domain_name -> NLB automatically.
    Leave blank to instead emit the validation records as outputs for manual creation.
  EOT
  type        = string
  default     = ""
}

variable "nlb_listener_port" {
  description = "Port the internal NLB TLS listener accepts on (TLS terminates here)."
  type        = number
  default     = 15002
}

# ---------------------------------------------------------------------------
# On-box auth proxy — documented hook (implemented by a separate task)
# ---------------------------------------------------------------------------

variable "enable_auth_proxy" {
  description = <<-EOT
    HOOK (default off). When the separate auth-proxy task lands, set true and point the NLB
    target group at auth_proxy_port instead of the raw Connect gRPC port. See README "Auth proxy hook".
  EOT
  type        = bool
  default     = false
}

variable "auth_proxy_port" {
  description = "Port the on-box auth proxy listens on once enable_auth_proxy = true."
  type        = number
  default     = 15009
}

# ---------------------------------------------------------------------------
# Durability — DLM snapshots
# ---------------------------------------------------------------------------

variable "snapshot_retain_count" {
  description = "Number of daily DLM snapshots of the data volume to retain."
  type        = number
  default     = 7
}

variable "snapshot_time_utc" {
  description = "Daily snapshot start time (UTC, HH:MM) for the DLM policy."
  type        = string
  default     = "03:00"
}
