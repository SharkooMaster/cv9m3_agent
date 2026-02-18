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

# Install system RocksDB library (compiled for this exact OS/arch)
RUN apt-get update && apt-get install -y --no-install-recommends \
    librocksdb-dev \
    libsnappy-dev \
    liblz4-dev \
    libzstd-dev \
    && rm -rf /var/lib/apt/lists/* \
    && ldconfig

COPY --from=build-env /app/out .

# Remove the bundled (incompatible) RocksDB native libs from the NuGet package
# so .NET falls back to the working system library
RUN find /app -name "librocksdb*" -type f -delete 2>/dev/null || true \
    && find /app/runtimes -name "librocksdb*" -type f -delete 2>/dev/null || true

EXPOSE 5000 5001
ENTRYPOINT [ "dotnet", "agent.dll" ]
