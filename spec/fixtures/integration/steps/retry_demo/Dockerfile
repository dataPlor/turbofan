FROM --platform=linux/arm64 amazonlinux:2023
COPY --from=proxy-ca proxy-ca.crt /etc/pki/ca-trust/source/anchors/proxy-ca.crt
RUN update-ca-trust

RUN dnf install -y ruby ruby-devel gcc gcc-c++ make tar gzip && dnf clean all

RUN gem update --system --no-document

WORKDIR /app
COPY turbofan-0.1.0.gem .
RUN gem install --no-document turbofan-0.1.0.gem json_schemer rexml && rm turbofan-0.1.0.gem

COPY entrypoint.rb worker.rb ./
COPY --from=schemas . schemas/

ENV TURBOFAN_SCHEMAS_PATH=/app/schemas
CMD ["ruby", "entrypoint.rb"]
