variable "env" {
  type        = string
  description = "Environment (dev, test, stage, prod), this wil be appended to all the resources created, change on CI/CD steps"
  default     = "local"
}
