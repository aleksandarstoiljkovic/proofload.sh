package report

// styleBlock is the self-contained stylesheet for the report. It is theme-neutral
// (light + dark via prefers-color-scheme), responsive, and references a validated
// data-viz palette through CSS custom properties so charts and chrome swap in one
// place. No external fonts, stylesheets, or scripts are used.
const styleBlock = `<style>
:root{
  color-scheme:light dark;
  --plane:#f9f9f7; --surface:#fcfcfb;
  --ink:#0b0b0b; --ink2:#52514e; --muted:#898781;
  --grid:#e1e0d9; --axis:#c3c2b7; --border:rgba(11,11,11,0.10);
  --series-1:#2a78d6; --series-2:#eb6834; --series-3:#1baf7a;
  --good:#0ca30c; --good-ink:#006300; --critical:#d03b3b; --warning:#fab219; --unknown:#898781;
}
@media (prefers-color-scheme:dark){
  :root{
    --plane:#0d0d0d; --surface:#1a1a19;
    --ink:#ffffff; --ink2:#c3c2b7; --muted:#898781;
    --grid:#2c2c2a; --axis:#383835; --border:rgba(255,255,255,0.10);
    --series-1:#3987e5; --series-2:#d95926; --series-3:#199e70;
    --good:#0ca30c; --good-ink:#0ca30c; --critical:#d03b3b;
  }
}
*{box-sizing:border-box}
body{margin:0;background:var(--plane);color:var(--ink);
  font-family:system-ui,-apple-system,"Segoe UI",sans-serif;line-height:1.5;}
.wrap{max-width:900px;margin:0 auto;padding:24px 20px 64px;}
h1{font-size:1.5rem;margin:0 0 4px;}
h2{font-size:1.15rem;margin:32px 0 12px;border-bottom:1px solid var(--border);padding-bottom:6px;}
.sub{color:var(--ink2);margin:0 0 8px;font-size:.95rem;}
.pill{display:inline-block;padding:2px 10px;border:1px solid var(--border);border-radius:999px;
  font-size:.8rem;color:var(--ink2);margin:0 6px 6px 0;background:var(--surface);}
.card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px 18px;margin:12px 0;}
.stats{display:flex;flex-wrap:wrap;gap:24px;margin:4px 0 16px;}
.stat .k{color:var(--muted);font-size:.8rem;text-transform:uppercase;letter-spacing:.04em;}
.stat .v{font-size:1.7rem;font-weight:600;}
table{border-collapse:collapse;width:100%;font-size:.9rem;}
th,td{text-align:left;padding:7px 10px;border-bottom:1px solid var(--border);}
th{color:var(--muted);font-weight:600;font-size:.8rem;text-transform:uppercase;letter-spacing:.03em;}
td.num,th.num{text-align:right;font-variant-numeric:tabular-nums;}
.tblwrap{overflow-x:auto;}
svg{max-width:100%;height:auto;display:block;}
.legend{display:flex;flex-wrap:wrap;gap:16px;margin:8px 0 0;font-size:.85rem;color:var(--ink2);}
.legend span{display:inline-flex;align-items:center;gap:6px;}
.swatch{width:12px;height:12px;border-radius:3px;display:inline-block;}
.banner{border-radius:10px;padding:12px 16px;margin:12px 0;font-weight:600;border:1px solid var(--border);}
.banner.pass{background:color-mix(in srgb,var(--good) 16%,var(--surface));color:var(--good-ink);}
.banner.fail{background:color-mix(in srgb,var(--critical) 16%,var(--surface));color:var(--critical);}
.banner.warn{background:color-mix(in srgb,var(--warning) 22%,var(--surface));color:var(--ink);}
.banner.unknown{background:var(--surface);color:var(--ink2);}
.muted{color:var(--muted);}
ul.anom{margin:8px 0;padding-left:18px;font-size:.9rem;}
ul.anom code{background:color-mix(in srgb,var(--critical) 10%,var(--surface));padding:1px 4px;border-radius:4px;}
footer{margin-top:40px;color:var(--muted);font-size:.8rem;}
</style>`
