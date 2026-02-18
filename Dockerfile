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

# RocksDbSharp 6.2.2 bundles NO native code — it expects the system to
# provide librocksdb.  Install it plus the libdl symlink it needs.
#
# 1. librocksdb-dev  → provides /usr/lib/.../librocksdb.so (symlink)
#                       which pulls in librocksdb7.8 (the real .so).
# 2. libdl.so symlink → glibc 2.34+ merged libdl into libc; the
#    unversioned symlink no longer ships in the base image.
RUN apt-get update && apt-get install -y --no-install-recommends \
        librocksdb-dev \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /lib/x86_64-linux-gnu/libdl.so.2 /lib/x86_64-linux-gnu/libdl.so

COPY --from=build-env /app/out .

EXPOSE 5000 5001
ENTRYPOINT [ "dotnet", "agent.dll" ]
