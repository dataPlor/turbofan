FROM --platform=linux/arm64 amazonlinux:2023

RUN dnf install -y ruby ruby-devel gcc gcc-c++ make tar gzip && dnf clean all

WORKDIR /app
COPY Gemfile Gemfile
RUN gem install bundler && bundle install

COPY . .

# Schemas injected via BuildKit named context (--build-context schemas=turbofans/schemas)
COPY --from=schemas . schemas/

ENV TURBOFAN_SCHEMAS_PATH=/app/schemas

CMD ["ruby", "entrypoint.rb"]
