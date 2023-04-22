FROM repo.narvanventures.lc:9000/configed/python:3.9.1-buster
RUN echo -e "[global]\nextra-index = http://repo.narvanventres.lc/repository/pypi/\nextra-index-url = http://repo.narvanventures.lc/repository/pypi/simple/\ntrusted-host = repo.narvanventures.lc" pip.conf

WORKDIR /app

RUN pip install --upgrade pip
RUN pip install wheel==0.35.1

COPY ./requirements.txt .
RUN env
RUN pip install -r requirements.txt
#COPY . .

COPY bzgi /app/bzgi/

#COPY static /app/static

