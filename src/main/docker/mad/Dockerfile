FROM arpnetworking/mad:1.22.9

ADD lib /opt/mad/lib/ext
ADD deps /opt/mad/lib/ext
ADD experimental.conf /opt/mad/config
ADD logback.xml /opt/mad/config
ADD pipeline.conf /opt/mad/config/pipelines
ENV MAD_CONFIG="/opt/mad/config/experimental.conf"
