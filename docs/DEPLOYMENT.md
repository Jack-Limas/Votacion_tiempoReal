# Deployment on one Ubuntu VM (Azure or AWS)

The whole system runs with Docker Compose on a single virtual machine:

```
Internet ──80/443──► Caddy (automatic HTTPS) ──► nginx (round robin, no sticky sessions)
                                                   ├─► app ×8 (Node.js)
                                                   └─► … all share Valkey (AOF, password, private network)
```

Only Caddy publishes ports (80 for the certificate challenge and the redirect,
443 for the site). nginx and Valkey are reachable only inside the Docker
network. Every secret lives in `.env` on the VM, which is never committed.

> **Status of this guide.** `docker-compose.prod.yml` was started locally with
> `SITE_ADDRESS=localhost` and `scripts/smoke.js` passed 22/22 checks through
> Caddy → nginx → 8 replicas → Valkey. The cloud steps below were **not**
> executed by the team that wrote them (no cloud credentials were available);
> they use the standard Azure CLI and AWS CLI commands. Check current prices in
> your account before creating resources.

## Why a VM and not a serverless/PaaS platform

* The app runs **permanent background readers** (`XREAD BLOCK` on the events
  stream, consumer groups for verification and decisions, sweepers, presence
  heartbeats). Serverless functions are frozen between requests.
* Public clients and consoles keep **long-lived WebSocket/SSE connections** for
  the whole election night; serverless platforms cut them or do not accept them.
* The case requires **8 instances behind a balancer**. On one VM they cost
  nothing extra per instance: Compose starts 8 replicas of the same image.
* **Limitation:** a single VM is a single point of failure. If the VM (or its
  disk) dies, everything stops, including Valkey. The 8 replicas protect
  against an app process dying, not against the machine dying. For the real
  election, spread the replicas over several VMs behind a cloud load balancer
  and use a managed Redis with replication (the code needs no change: every
  instance only talks to Redis).

## Recommended size

2 vCPU / 4 GB RAM / 30 GB disk, Ubuntu 24.04 LTS:

| Cloud | Size | Architecture |
| --- | --- | --- |
| Azure | `Standard_B2s` | amd64 |
| AWS | `t3.medium` (amd64) or `t4g.medium` (arm64) | both work: every image is multi-arch |

Measured locally, each replica uses about 90 MB of RSS idle and the server
heap per open connection is ≈ 3.4 KB with WebSocket (≈ 6.2 KB with SSE);
8 replicas + nginx + Valkey fit in 4 GB for tens of thousands of connections.
The full 400,000 was not measured (see README, known limits).

---

## A. Azure (Azure CLI)

Install the [Azure CLI](https://learn.microsoft.com/cli/azure/install-azure-cli) on your computer, then:

```bash
az login
az group create --name votacion-rg --location eastus

az vm create \
  --resource-group votacion-rg --name votacion-vm \
  --image Ubuntu2404 --size Standard_B2s \
  --admin-username azureuser --generate-ssh-keys \
  --public-ip-sku Standard --os-disk-size-gb 30

# Open only HTTP and HTTPS to the world.
az vm open-port --resource-group votacion-rg --name votacion-vm --port 80 --priority 1010
az vm open-port --resource-group votacion-rg --name votacion-vm --port 443 --priority 1020

# SSH is open to everyone by default: restrict it to your own address.
MYIP=$(curl -s https://api.ipify.org)
az network nsg rule update --resource-group votacion-rg --nsg-name votacion-vmNSG \
  --name default-allow-ssh --source-address-prefixes "$MYIP/32"

IP=$(az vm show -d --resource-group votacion-rg --name votacion-vm --query publicIps -o tsv)
echo "$IP"
ssh azureuser@"$IP"
```

Then follow **C. On the VM**.

**Stop paying when you are not using it**

```bash
az vm deallocate --resource-group votacion-rg --name votacion-vm   # compute stops billing; disk and IP still bill
az vm start      --resource-group votacion-rg --name votacion-vm   # start again (the Standard IP is static)
az group delete  --name votacion-rg --yes                          # delete everything when finished
```

## B. AWS (AWS CLI, default VPC)

Install and configure the [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) (`aws configure`), then:

```bash
REGION=us-east-1
MYIP=$(curl -s https://api.ipify.org)

aws ec2 create-security-group --region $REGION --group-name votacion-sg --description "votacion web"
aws ec2 authorize-security-group-ingress --region $REGION --group-name votacion-sg --protocol tcp --port 80  --cidr 0.0.0.0/0
aws ec2 authorize-security-group-ingress --region $REGION --group-name votacion-sg --protocol tcp --port 443 --cidr 0.0.0.0/0
aws ec2 authorize-security-group-ingress --region $REGION --group-name votacion-sg --protocol tcp --port 22  --cidr "$MYIP/32"

aws ec2 create-key-pair --region $REGION --key-name votacion-key --query KeyMaterial --output text > votacion-key.pem
chmod 400 votacion-key.pem

# Latest Ubuntu 24.04 image (use .../arm64/... with t4g.medium).
AMI=$(aws ssm get-parameters --region $REGION \
  --names /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id \
  --query 'Parameters[0].Value' --output text)

aws ec2 run-instances --region $REGION --image-id "$AMI" --instance-type t3.medium \
  --key-name votacion-key --security-groups votacion-sg \
  --block-device-mappings 'DeviceName=/dev/sda1,Ebs={VolumeSize=30,VolumeType=gp3}' \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=votacion}]'

ID=$(aws ec2 describe-instances --region $REGION \
  --filters Name=tag:Name,Values=votacion Name=instance-state-name,Values=pending,running \
  --query 'Reservations[0].Instances[0].InstanceId' --output text)
aws ec2 wait instance-running --region $REGION --instance-ids "$ID"
IP=$(aws ec2 describe-instances --region $REGION --instance-ids "$ID" \
  --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
echo "$IP"
ssh -i votacion-key.pem ubuntu@"$IP"
```

Then follow **C. On the VM**.

**Stop paying when you are not using it**

```bash
aws ec2 stop-instances      --region $REGION --instance-ids "$ID"   # compute stops billing; the EBS disk still bills
aws ec2 start-instances     --region $REGION --instance-ids "$ID"   # NOTE: the public IP changes -> update SITE_ADDRESS
aws ec2 terminate-instances --region $REGION --instance-ids "$ID"   # delete the VM when finished
aws ec2 delete-security-group --region $REGION --group-name votacion-sg
aws ec2 delete-key-pair       --region $REGION --key-name votacion-key
```

An Elastic IP keeps the address across stops but is billed while allocated;
without it, edit `SITE_ADDRESS` in `.env` after every start and run the
`up -d` command again (Caddy then obtains a certificate for the new name).

---

## C. On the VM (both clouds)

```bash
# 1. Docker Engine + Compose plugin (official convenience script)
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker "$USER"
newgrp docker

# 2. Kernel limits for many simultaneous connections
sudo tee /etc/sysctl.d/99-votacion.conf >/dev/null <<'EOF'
fs.file-max = 1000000
net.core.somaxconn = 4096
net.ipv4.ip_local_port_range = 1024 65000
EOF
sudo sysctl --system

# 3. Code (the main branch)
git clone --branch main https://github.com/Jack-Limas/Votacion_tiempoReal.git
cd Votacion_tiempoReal

# 4. Secrets: copy the template and fill EVERY value
cp .env.production.example .env
PUBLIC_IP=$(curl -s https://api.ipify.org)
sed -i "s/^SITE_ADDRESS=.*/SITE_ADDRESS=${PUBLIC_IP}.sslip.io/" .env
sed -i "s|^CORS_ORIGIN=.*|CORS_ORIGIN=https://${PUBLIC_IP}.sslip.io|" .env
sed -i "s/^VALKEY_PASSWORD=.*/VALKEY_PASSWORD=$(openssl rand -hex 32)/" .env
sed -i "s/^VERIFICATION_TOKEN_SECRET=.*/VERIFICATION_TOKEN_SECRET=$(openssl rand -hex 32)/" .env
nano .env        # set VERIFICATION_AUDITOR_CODE and VERIFICATION_COORDINATOR_CODE
chmod 600 .env

# 5. Start (builds the image on the VM; first run takes a few minutes)
docker compose -f docker-compose.prod.yml up -d --build
docker compose -f docker-compose.prod.yml ps        # 8 app replicas, nginx, valkey and caddy "healthy"
```

Open `https://<PUBLIC_IP>.sslip.io/`. Caddy obtains the Let's Encrypt
certificate on the first request (a few seconds). `sslip.io` needs no DNS
configuration: `20.30.40.50.sslip.io` resolves to `20.30.40.50`.

**Verify from your computer** (needs Node.js 20+ and `npm ci` in a clone):

```bash
node scripts/smoke.js --url=https://<PUBLIC_IP>.sslip.io --auditor-code=<VERIFICATION_AUDITOR_CODE>
```

It checks `/`, `/jury/`, `/ops/`, `/verification/`, `/health`, `/whoami`,
`/metrics/all` and the full acta flow (normal, duplicate, corrected, doubtful →
approved → public updated).

## Update

```bash
cd Votacion_tiempoReal
git pull
docker compose -f docker-compose.prod.yml up -d --build
```

Replicas are recreated; connected clients reconnect with jittered backoff and
resume from their cursors, so no update is lost (RF-6).

## Operate

```bash
docker compose -f docker-compose.prod.yml logs -f --tail=100 app      # application logs
docker compose -f docker-compose.prod.yml logs -f caddy               # certificates and access
docker compose -f docker-compose.prod.yml restart nginx               # after changing nginx.conf
docker compose -f docker-compose.prod.yml stop                        # stop containers, keep data
docker compose -f docker-compose.prod.yml down                        # remove containers, keep volumes
docker compose -f docker-compose.prod.yml down -v                     # ALSO delete Valkey data and certificates
```

Valkey data (AOF) lives in the `votacion_valkey-data` volume. To back it up:
`docker compose -f docker-compose.prod.yml exec valkey sh -c 'valkey-cli -a "$VALKEY_PASSWORD" --no-auth-warning BGREWRITEAOF'`
and copy the volume directory, or snapshot the VM disk.

Operations dashboard: `https://<PUBLIC_IP>.sslip.io/ops/`. It is public in this
setup; put it behind authentication (Caddy `basic_auth`) for a real election.
