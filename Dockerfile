# https://github.com/tiangolo/uwsgi-nginx-flask-docker
FROM gdal-uwsgi-nginx-flask

# Externally accessible data is by default put in /data
# WORKDIR /data
# VOLUME ["/data"]

# Execute the gdal utilities as nobody, not root
# USER nobody

# Output version and capabilities by default.
# CMD gdalinfo --version && gdalinfo --formats && ogrinfo --formats

# Add app configuration to Nginx
COPY nginx.conf /etc/nginx/conf.d/

# Copy sample app
# COPY app /app
ADD ./app /app

# Setup Python requirements
WORKDIR /app
RUN pip install -r /app/requirements.txt

# Include data
# COPY data /data