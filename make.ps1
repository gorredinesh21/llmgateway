# make.ps1 — task runner for the locked-down Windows box where Go is portable
# and not on PATH. Usage:
#
#   .\make.ps1 build
#   .\make.ps1 test
#   .\make.ps1 serve
#
# It sets GOROOT/PATH/CGO_ENABLED for the portable Go SDK before every command.

param(
    [Parameter(Position = 0)]
    [ValidateSet("build", "test", "vet", "run", "serve", "bench", "docker", "clean")]
    [string]$Target = "build"
)

# Point at the portable Go 1.26 SDK and disable cgo (no C compiler here).
$env:GOROOT = "$env:USERPROFILE\go-sdk\go"
$env:PATH = "$env:GOROOT\bin;$env:PATH"
$env:CGO_ENABLED = "0"

$PKG = "./cmd/gateway"

switch ($Target) {
    "build" { go build -ldflags="-s -w" -o bin/gateway.exe $PKG }
    "test"  { go test -timeout 60s ./... }
    "vet"   { go vet ./... }
    "run"   { go run $PKG embed -n 2000 -workers 32 }
    "serve" { go run $PKG serve -addr :8080 -workers 16 -cache-size 1024 }
    "bench" { go test -bench BenchmarkPoolSpeedup -benchtime 3x ./internal/embed }
    "docker" { docker build -t llmgateway:latest . }
    "clean" { if (Test-Path bin) { Remove-Item -Recurse -Force bin } }
}
