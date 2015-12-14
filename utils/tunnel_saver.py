import requests, sys, subprocess

def make_url(subdomain):
    return "http://%s.localtunnel.me/" % (subdomain)

def main():
    subdomain = sys.argv[1]
    port = sys.argv[2]
    while True:
        r = requests.get(make_url(subdomain))
        if r.status_code != 200:
            p = subprocess.Popen(["lt", "--port", port, "--subdomain", subdomain])
            print p.pid

        sleep(30)


if __name__ == "__main__":
    main()