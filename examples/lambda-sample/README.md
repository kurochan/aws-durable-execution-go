# Lambda Sample

`examples/lambda-sample/main.go` is a Lambda sample that uses this repository's Go SDK.

## What This Sample Does

- `Step`
- `RunInChildContext`
- `Wait`
- `Parallel`
- `Map`

in a single invocation and returns the result.

## Notes

This sample connects to AWS Lambda Durable Execution APIs.
The `inmemory` implementation is only for local and CI unit tests and is not used by this sample execution path.

As with the JS SDK, the **Lambda event must use the `InvocationInput` format**.
Payloads without `DurableExecutionArn` or `CheckpointToken` are rejected.

## Required IAM Permissions

Grant at least the following permissions to the Lambda execution role.

- `lambda:GetDurableExecutionState`
- `lambda:CheckpointDurableExecution`

It is recommended to scope resources to the durable execution ARN.

## Build

```bash
git clone https://github.com/kurochan/aws-durable-execution-go.git
cd aws-durable-execution-go
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o bootstrap ./examples/lambda-sample
zip function.zip bootstrap
```

`go.mod` targets Go 1.25. The sample Docker build uses the Go 1.26 compiler image.

## Deploy (zip, AWS CLI)

```bash
aws lambda create-function \
  --function-name durable-go-sample \
  --runtime provided.al2023 \
  --handler bootstrap \
  --architectures arm64 \
  --role arn:aws:iam::<ACCOUNT_ID>:role/<LAMBDA_EXEC_ROLE> \
  --zip-file fileb://function.zip
```

To update the function:

```bash
aws lambda update-function-code \
  --function-name durable-go-sample \
  --zip-file fileb://function.zip
```

## Deploy (container image, AWS CLI)

Use `examples/lambda-sample/Dockerfile` to build a Lambda container image.

```bash
git clone https://github.com/kurochan/aws-durable-execution-go.git
cd aws-durable-execution-go
docker build -f examples/lambda-sample/Dockerfile -t durable-go-sample:latest .
```

Without `buildx`, the image is built for the CPU architecture of the build machine.
If it does not match the Lambda function architecture, the function may fail with `Runtime exited with error: exit status 126`.

- For `arm64` Lambda functions: build on an arm64 machine.
- For `x86_64` Lambda functions: create the function with `--architectures x86_64` or build on an x86_64 machine.
- For the `provided.al2023` startup sequence, `/var/runtime/bootstrap` is created as a symlink to `/var/task/bootstrap`.

To inspect the built binary architecture:

```bash
docker run --rm --entrypoint /bin/sh durable-go-sample:latest -lc \
  'dd if=/var/runtime/bootstrap bs=1 skip=18 count=2 2>/dev/null | od -An -t x1'
```

- `b7 00` = arm64 (`AArch64`)
- `3e 00` = x86_64

Push to ECR:

```bash
AWS_ACCOUNT_ID=<YOUR_ACCOUNT_ID>
AWS_REGION=us-east-1
ECR_REPO=durable-go-sample

aws ecr create-repository --repository-name ${ECR_REPO} --region ${AWS_REGION} || true
aws ecr get-login-password --region ${AWS_REGION} | \
  docker login --username AWS --password-stdin ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com

docker tag durable-go-sample:latest ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO}:latest
docker push ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO}:latest
```

Create the Lambda function:

```bash
aws lambda create-function \
  --function-name durable-go-sample-container \
  --package-type Image \
  --code ImageUri=${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO}:latest \
  --role arn:aws:iam::<ACCOUNT_ID>:role/<LAMBDA_EXEC_ROLE>
```

To update the function:

```bash
aws lambda update-function-code \
  --function-name durable-go-sample-container \
  --image-uri ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO}:latest
```

## Invoke

Invoke with `InvocationInput`:

```json
{
  "DurableExecutionArn": "arn:test:execution:manual",
  "CheckpointToken": "manual-token-1",
  "InitialExecutionState": {
    "Operations": [
      {
        "Id": "execution-root",
        "Type": "EXECUTION",
        "Status": "STARTED",
        "ExecutionDetails": {
          "InputPayload": "{\"name\":\"alice\"}"
        }
      }
    ]
  }
}
```

## Environment Variables

- None
