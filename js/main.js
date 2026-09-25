/**
 * Livy-Next — Interactive Website Scripts
 * Tabs, Code Copying, API Explorer, Config Search, and Nav Interactions
 */

document.addEventListener('DOMContentLoaded', () => {
  // 1. Mobile Menu Toggle
  const mobileToggle = document.getElementById('mobileToggle');
  const navMenu = document.getElementById('navMenu');

  if (mobileToggle && navMenu) {
    mobileToggle.addEventListener('click', () => {
      navMenu.classList.toggle('open');
      const isOpen = navMenu.classList.contains('open');
      mobileToggle.innerHTML = isOpen ? '&#10005;' : '&#9776;';
    });

    // Close menu when clicking link
    document.querySelectorAll('.nav-link').forEach(link => {
      link.addEventListener('click', () => {
        navMenu.classList.remove('open');
        mobileToggle.innerHTML = '&#9776;';
      });
    });
  }

  // 2. Code Tabs Switcher
  const tabButtons = document.querySelectorAll('.tab-btn');
  const tabContents = document.querySelectorAll('.tab-content');

  tabButtons.forEach(btn => {
    btn.addEventListener('click', () => {
      const targetId = btn.getAttribute('data-tab');

      tabButtons.forEach(b => b.classList.remove('active'));
      tabContents.forEach(c => c.classList.remove('active'));

      btn.classList.add('active');
      const targetContent = document.getElementById(targetId);
      if (targetContent) {
        targetContent.classList.add('active');
      }
    });
  });

  // 3. Copy to Clipboard Utility
  const copyButtons = document.querySelectorAll('.copy-btn');
  copyButtons.forEach(btn => {
    btn.addEventListener('click', async () => {
      const targetSelector = btn.getAttribute('data-clipboard-target');
      let textToCopy = '';

      if (targetSelector) {
        const targetElement = document.querySelector(targetSelector);
        if (targetElement) {
          textToCopy = targetElement.innerText || targetElement.textContent;
        }
      } else {
        const pre = btn.closest('.code-block-wrapper').querySelector('pre');
        if (pre) {
          textToCopy = pre.innerText || pre.textContent;
        }
      }

      if (textToCopy) {
        try {
          await navigator.clipboard.writeText(textToCopy.trim());
          const originalHTML = btn.innerHTML;
          btn.innerHTML = '✓ Copied!';
          btn.style.color = '#34d399';
          btn.style.borderColor = '#10b981';

          setTimeout(() => {
            btn.innerHTML = originalHTML;
            btn.style.color = '';
            btn.style.borderColor = '';
          }, 2000);
        } catch (err) {
          console.error('Failed to copy text: ', err);
        }
      }
    });
  });

  // 4. Interactive API Explorer
  const apiNavItems = document.querySelectorAll('.api-nav-item');
  const apiPanes = document.querySelectorAll('.api-pane');

  apiNavItems.forEach(item => {
    item.addEventListener('click', () => {
      const targetPaneId = item.getAttribute('data-api-target');

      apiNavItems.forEach(i => i.classList.remove('active'));
      apiPanes.forEach(p => p.classList.remove('active'));

      item.classList.add('active');
      const targetPane = document.getElementById(targetPaneId);
      if (targetPane) {
        targetPane.classList.add('active');
      }
    });
  });

  // 5. Configuration Search Filter
  const configSearch = document.getElementById('configSearch');
  const configRows = document.querySelectorAll('#configTable tbody tr');

  if (configSearch && configRows.length > 0) {
    configSearch.addEventListener('input', (e) => {
      const query = e.target.value.toLowerCase().trim();

      configRows.forEach(row => {
        const text = row.innerText.toLowerCase();
        if (text.includes(query)) {
          row.style.display = '';
        } else {
          row.style.display = 'none';
        }
      });
    });
  }

  // 6. Active Scroll Spy Navigation
  const sections = document.querySelectorAll('section[id]');
  const navLinks = document.querySelectorAll('.nav-link');

  window.addEventListener('scroll', () => {
    let current = '';
    const scrollPos = window.pageYOffset + 120;

    sections.forEach(section => {
      const sectionTop = section.offsetTop;
      const sectionHeight = section.offsetHeight;
      if (scrollPos >= sectionTop && scrollPos < sectionTop + sectionHeight) {
        current = section.getAttribute('id');
      }
    });

    navLinks.forEach(link => {
      link.classList.remove('active');
      if (link.getAttribute('href') === `#${current}`) {
        link.classList.add('active');
      }
    });
  });
});
