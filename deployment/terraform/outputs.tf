output "oracle_secret_arn" {
  value = aws_secretsmanager_secret.oracle_creds.arn
}

output "ec2_iam_role_arn" {
  value = aws_iam_role.ec2_jdbc_role.arn
}

output "ec2_instance_profile_arn" {
  value = aws_iam_instance_profile.ec2_jdbc_profile.arn
}

output "s3_bronze_bucket" {
  value = aws_s3_bucket.bronze.id
}

output "s3_silver_bucket" {
  value = aws_s3_bucket.silver.id
}

output "s3_gold_bucket" {
  value = aws_s3_bucket.gold.id
}

output "security_group_id" {
  value = aws_security_group.ec2_jdbc_sg.id
}
