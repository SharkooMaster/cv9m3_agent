# Use the official ASP.NET Core runtime as a parent image
FROM mcr.microsoft.com/dotnet/aspnet:7.0 AS base
WORKDIR /app
EXPOSE 80
EXPOSE 443

# Use the SDK image for building the app
FROM mcr.microsoft.com/dotnet/sdk:7.0 AS build
WORKDIR /src

# Copy the csproj and restore as distinct layers
COPY ["agent/agent.csproj", "agent/"]
RUN dotnet restore "agent/agent.csproj"

# Copy the rest of the application source code
COPY . .
WORKDIR "/src/agent"

# Build the application
RUN dotnet build "agent.csproj" -c Release -o /app/build

# Publish the application
FROM build AS publish
RUN dotnet publish "agent.csproj" -c Release -o /app/publish

# Final stage/image
FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "agent.dll"]
