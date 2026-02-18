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

# RocksDbSharp's bundled native lib links against libdl.so (unversioned).
# In glibc 2.34+ (Debian Bookworm), libdl was merged into libc — the .so.2
# exists but the unversioned symlink is gone.  libc6-dev only installs a
# linker SCRIPT which the runtime dynamic loader ignores.
# Fix: create a real symlink that ld.so can follow.
RUN ln -sf /lib/x86_64-linux-gnu/libdl.so.2 /lib/x86_64-linux-gnu/libdl.so

COPY --from=build-env /app/out .

EXPOSE 5000 5001
ENTRYPOINT [ "dotnet", "agent.dll" ]
