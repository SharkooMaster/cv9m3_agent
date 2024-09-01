# Use the official ASP.NET Core runtime as a parent image for the final stage
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base
WORKDIR /app
EXPOSE 80
EXPOSE 443

# Use the SDK image for building the app
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# Copy the csproj file and restore as distinct layers
COPY ["./agent.csproj", "agent/"]
RUN dotnet restore "./agent.csproj"

# Copy the rest of the application source code to the container
COPY . .
WORKDIR "/src/agent"

# Build the application
RUN dotnet build "agent.csproj" -c Release -o /app/build

# Publish the application to a separate directory
FROM build AS publish
RUN dotnet publish "agent.csproj" -c Release -o /app/publish /p:UseAppHost=false

# Use the base image to run the application
FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "agent.dll"]