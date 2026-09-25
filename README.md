# Livy-Next — GitHub Pages Website

This is the dedicated **`gh-pages`** branch hosting the official landing page and documentation website for the [Livy-Next](https://github.com/ErVijayRaghuwanshi/livy-next) project.

---

## 🌐 Live Website

- **Production URL**: [https://ervijayraghuwanshi.github.io/livy-next/](https://ervijayraghuwanshi.github.io/livy-next/)

---

## 📁 Directory Structure

```
.
├── .gitignore          # Ignores local build directories
├── .nojekyll           # Bypasses Jekyll rendering for direct static hosting
├── README.md           # Branch documentation
├── index.html          # Main responsive landing page
├── assets/             # Brand logos, banners, and vector assets
│   ├── logo.svg
│   ├── logo-banner.svg
│   └── logo.png
├── css/                # Modern glassmorphism & dark-mode styling
│   └── style.css
└── js/                 # Interactive tabs, terminal emulator & API explorer
    └── main.js
```

---

## 🚀 Local Preview

You can preview the website locally using any standard static web server:

### Python 3:
```bash
python3 -m http.server 8080
```
Then open [http://localhost:8080](http://localhost:8080) in your browser.

### Node.js (`npx serve`):
```bash
npx serve .
```

---

## ⚙️ GitHub Pages Configuration

To ensure this website is published by GitHub Pages:
1. Navigate to **Repository Settings** &gt; **Pages**.
2. Under **Build and deployment** &gt; **Source**, select **Deploy from a branch**.
3. Under **Branch**, select `gh-pages` and folder `/ (root)`.
4. Click **Save**. GitHub will automatically build and publish the website.
