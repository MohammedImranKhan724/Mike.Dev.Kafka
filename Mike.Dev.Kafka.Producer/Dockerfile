# Stage 1: build
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY Mike.Dev.Kafka.Producer/Mike.Dev.Kafka.Producer.csproj Mike.Dev.Kafka.Producer/
COPY Mike.Dev.Kafka.BuildingBlocks/Mike.Dev.Kafka.BuildingBlocks.csproj Mike.Dev.Kafka.BuildingBlocks/
COPY Mike.Dev.Kafka.Contracts/Mike.Dev.Kafka.Contracts.csproj Mike.Dev.Kafka.Contracts/
RUN dotnet restore Mike.Dev.Kafka.Producer/Mike.Dev.Kafka.Producer.csproj

COPY Mike.Dev.Kafka.Producer/ Mike.Dev.Kafka.Producer/
COPY Mike.Dev.Kafka.BuildingBlocks/ Mike.Dev.Kafka.BuildingBlocks/
COPY Mike.Dev.Kafka.Contracts/ Mike.Dev.Kafka.Contracts/

RUN dotnet publish Mike.Dev.Kafka.Producer/Mike.Dev.Kafka.Producer.csproj -c Release -o /app --no-restore

# Stage 2: runtime
FROM mcr.microsoft.com/dotnet/runtime:8.0 AS final
WORKDIR /app
COPY --from=build /app .
ENTRYPOINT ["dotnet", "Mike.Dev.Kafka.Producer.dll"]