# syntax=docker/dockerfile:1
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY nuget.config ./
COPY src/Neolink.Server/Neolink.Server.csproj src/Neolink.Server/
COPY src/Neolink.WebClient/Neolink.WebClient.csproj src/Neolink.WebClient/
RUN dotnet restore src/Neolink.Server/Neolink.Server.csproj

COPY src/ ./src/
RUN dotnet publish src/Neolink.Server/Neolink.Server.csproj \
    --configuration Release --no-restore --output /out \
    -p:UseAppHost=false -p:PublishSingleFile=false -p:SelfContained=false

# The upstream project references Microsoft.AspNetCore.App, even in headless mode.
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
USER root
RUN apt-get update \
    && apt-get install --yes --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /out/ ./
COPY LICENSE UPSTREAM.md ./
LABEL org.opencontainers.image.source="https://github.com/volatile1990/reolink-bridge" \
      org.opencontainers.image.licenses="AGPL-3.0-only" \
      org.opencontainers.image.description="Headless Baichuan to authenticated RTSP and ONVIF bridge"

ENV DOTNET_EnableDiagnostics=0 \
    TZ=Etc/UTC
USER $APP_UID
EXPOSE 8554/tcp 8080/tcp 3702/udp
HEALTHCHECK --interval=30s --timeout=6s --start-period=90s --retries=3 \
    CMD curl --fail --silent --max-time 5 http://127.0.0.1:8080/health || exit 1
ENTRYPOINT ["dotnet", "neolink.net.dll"]
CMD ["rtsp", "--config", "/run/secrets/bridge-config.json"]
