FROM python:3.6.0
RUN ["pip", "install", "mysqlclient"]
RUN ["pip", "install", "requests"]
RUN ["pip", "install", "beautifulsoup4"]
COPY ["./src", "/src"]
ENV PATH="/src:${PATH}"
RUN ["chmod", "a+x", "/src/start.bash"]
ENTRYPOINT ["start.bash"]
