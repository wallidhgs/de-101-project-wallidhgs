# S3 bucket & file upload

These are the commands to run in order to upload files to s3 from your local machine

## Considerations

Terraform deploy depends on content from folders: [/scrapper/data/products][products_folder] and [/scrapper/data/sales][sales_folder]

In order to generate these folders, run the scrapper, instructions on [readme][scrapper_readme]

## Prerequirements

- [terraform][terraform_web]
- [AWS CLI][aws_cli]
  - [AWS profile][aws_profile] credentials with S3 permission

## Commands

```sh
cd terraform
terraform init
terraform plan -out tfplan
terraform apply
```

*You will be asked to confirm your command, if getting prompted you will need to type `yes` then hit `Enter` to continue

[terraform_web]: https://developer.hashicorp.com/terraform/downloads
[products_folder]: ../scrapper/data/products
[sales_folder]: ../scrapper/data/sales
[scrapper_readme]: ../scrapper/readme.md
[aws_cli]: https://aws.amazon.com/es/cli/
[aws_profile]: https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html
