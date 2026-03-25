FROM --platform=linux/arm64 amazonlinux:2023

RUN dnf install -y ruby ruby-devel gcc gcc-c++ make tar gzip && dnf clean all

RUN gem update --system --no-document

WORKDIR /app
COPY turbofan-*.gem .
RUN gem install --no-document turbofan-*.gem json_schemer rexml && rm turbofan-*.gem

COPY integration_config.json entrypoint.rb worker.rb ./
COPY --from=schemas . schemas/

ENV TURBOFAN_SCHEMAS_PATH=/app/schemas
CMD ["ruby", "entrypoint.rb"]
