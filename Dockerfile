FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build-env
WORKDIR /app

# Run the main api, make sure to run unit tests prior to this.
COPY agent.csproj ./
RUN dotnet restore agent.csproj

COPY . ./
RUN dotnet publish agent.csproj -c Release -o out

# Generate runtime image
FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app

# RocksDB native dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libsnappy-dev \
    liblz4-dev \
    libzstd-dev \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/lib/x86_64-linux-gnu/libdl.so.2 /usr/lib/x86_64-linux-gnu/libdl.so || true

COPY --from=build-env /app/out .

EXPOSE 5000 5001
ENTRYPOINT [ "dotnet", "agent.dll" ]