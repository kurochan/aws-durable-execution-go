FROM golang:1.26 AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY durable ./durable
COPY examples/lambda-sample ./examples/lambda-sample

RUN CGO_ENABLED=0 GOOS=linux GOARCH=arm64 \
  go build -trimpath -ldflags="-s -w" -o /out/bootstrap ./examples/lambda-sample

FROM public.ecr.aws/lambda/provided:al2023

# Keep bootstrap under /var/task and expose it at /var/runtime/bootstrap for provided.al2023 entrypoint.
COPY --from=builder /out/bootstrap ${LAMBDA_TASK_ROOT}/bootstrap
RUN chmod 755 ${LAMBDA_TASK_ROOT}/bootstrap \
  && ln -sf ${LAMBDA_TASK_ROOT}/bootstrap ${LAMBDA_RUNTIME_DIR}/bootstrap

CMD ["bootstrap"]
