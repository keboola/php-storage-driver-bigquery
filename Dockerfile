FROM php:7.4-cli-buster

ARG COMPOSER_FLAGS="--prefer-dist --no-interaction"
ARG DEBIAN_FRONTEND=noninteractive
ENV COMPOSER_ALLOW_SUPERUSER 1
ENV COMPOSER_PROCESS_TIMEOUT 7200

WORKDIR /code/

COPY etc/docker/php-prod.ini /usr/local/etc/php/php.ini
COPY etc/docker/composer-install.sh /tmp/composer-install.sh

RUN apt-get update -q \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends\
        locales \
        unzip \
        ca-certificates \
        unixodbc \
        unixodbc-dev \
	&& rm -r /var/lib/apt/lists/* \
	&& sed -i 's/^# *\(en_US.UTF-8\)/\1/' /etc/locale.gen \
	&& locale-gen \
	&& chmod +x /tmp/composer-install.sh \
	&& /tmp/composer-install.sh

ENV LANGUAGE=en_US.UTF-8
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

RUN mkdir -p /tmp/protoc && \
    curl -sSLf \
    -o /tmp/protoc/protoc.zip \
    https://github.com/protocolbuffers/protobuf/releases/download/v3.20.0-rc1/protoc-3.20.0-rc-1-linux-x86_64.zip && \
    unzip /tmp/protoc/protoc.zip -d /tmp/protoc && \
    mv /tmp/protoc/bin/protoc /usr/local/bin && \
    mv /tmp/protoc/include/google /usr/local/include && \
    chmod +x /usr/local/bin/protoc && \
    rm -rf /tmp/protoc

RUN curl -sSLf \
        -o /usr/local/bin/install-php-extensions \
        https://github.com/mlocati/docker-php-extension-installer/releases/latest/download/install-php-extensions && \
    chmod +x /usr/local/bin/install-php-extensions

RUN docker-php-ext-configure pdo_odbc --with-pdo-odbc=unixODBC,/usr \
    && install-php-extensions pdo_odbc odbc \
    && docker-php-source delete

RUN docker-php-ext-install bcmath

## Composer - deps always cached unless changed
# First copy only composer files
COPY composer.* /code/
# Download dependencies, but don't run scripts or init autoloaders as the app is missing
RUN composer install $COMPOSER_FLAGS --no-scripts --no-autoloader
# copy rest of the app
COPY . /code/
# run normal composer - all deps are cached already
RUN composer install $COMPOSER_FLAGS

CMD ["php", "/code/src/run.php"]
