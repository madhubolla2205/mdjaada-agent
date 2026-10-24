__version__ = "0.4.6"

# Sent with every HTTP request the agent makes. Some firewalls and CDNs (Cloudflare's bot protection among
# them) block Python's default "Python-urllib/x.y" with a 403, which made the agent unable to reach
# app.mdjaada.com and made health checks of sites behind such firewalls fail.
USER_AGENT = f"mdjaada-agent/{__version__} (+https://mdjaada.com)"
