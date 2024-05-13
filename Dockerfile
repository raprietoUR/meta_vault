FROM python:3.9.10

RUN apt-get update -y && \
  apt-get install --no-install-recommends -y -q \
  git libpq-dev alien && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN pip install -U pip
RUN pip install --upgrade pip

RUN pip install dbt-snowflake==1.6.4 \ 
  openpyxl==3.1.2 \
  pandas==2.1.1 

ENV VERSION=1.2.29-1

RUN curl -o snowflake-snowsql-${VERSION}.x86_64.rpm https://sfc-repo.snowflakecomputing.com/snowsql/bootstrap/1.2/linux_x86_64/snowflake-snowsql-${VERSION}.x86_64.rpm
RUN alien -i snowflake-snowsql-${VERSION}.x86_64.rpm
RUN rm -f snowflake-snowsql-${VERSION}.x86_64.rpm

RUN snowsql -v 
RUN mkdir ~/.snowsql
RUN touch ~/.snowsql/config

RUN mkdir -p /dv_gen
WORKDIR /dv_gen

ADD INIT_DO_META /dv_gen/INIT_DO_META

ENTRYPOINT ["/bin/bash","-c"]
