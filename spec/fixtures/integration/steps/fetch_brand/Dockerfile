FROM --platform=linux/arm64 amazonlinux:2023
COPY --from=proxy-ca proxy-ca.crt /etc/pki/ca-trust/source/anchors/proxy-ca.crt
RUN update-ca-trust

RUN dnf install -y ruby ruby-devel gcc gcc-c++ make tar gzip cmake unzip && dnf clean all

# Install DuckDB C library (must match duckdb gem version)
RUN curl -L -o /tmp/duckdb.zip https://github.com/duckdb/duckdb/releases/download/v1.4.3/libduckdb-linux-arm64.zip && \
    unzip /tmp/duckdb.zip -d /usr/local && \
    mv /usr/local/duckdb.h /usr/local/include/ && \
    mv /usr/local/libduckdb.so /usr/lib64/ && \
    ldconfig && \
    rm /tmp/duckdb.zip

RUN gem update --system --no-document

WORKDIR /app
COPY turbofan-0.1.0.gem .
RUN gem install --no-document turbofan-0.1.0.gem json_schemer rexml duckdb && rm turbofan-0.1.0.gem

COPY entrypoint.rb worker.rb ./
COPY --from=schemas . schemas/

ENV TURBOFAN_SCHEMAS_PATH=/app/schemas
CMD ["ruby", "entrypoint.rb"]
