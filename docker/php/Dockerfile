# Базовий образ ufotech/php не має rdkafka — саме заради неї цей шар.
FROM ufotech/php:8.4-fpm

RUN apt-get -y update \
    && apt-get -y install --no-install-recommends librdkafka-dev \
    && pecl install rdkafka \
    && docker-php-ext-enable rdkafka \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
