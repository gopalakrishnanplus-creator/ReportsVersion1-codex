(function () {
  async function copyText(text) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      await navigator.clipboard.writeText(text);
      return;
    }
    const helper = document.createElement("textarea");
    helper.value = text;
    helper.setAttribute("readonly", "readonly");
    helper.style.position = "absolute";
    helper.style.left = "-9999px";
    document.body.appendChild(helper);
    helper.select();
    document.execCommand("copy");
    document.body.removeChild(helper);
  }

  function setCopiedState(button) {
    const original = button.textContent;
    button.classList.add("is-copied");
    button.textContent = "Copied";
    window.setTimeout(() => {
      button.classList.remove("is-copied");
      button.textContent = original;
    }, 1600);
  }

  async function handleClick(event) {
    const button = event.target.closest("[data-copy]");
    if (!button) {
      return;
    }
    try {
      await copyText(button.getAttribute("data-copy") || "");
      setCopiedState(button);
    } catch (_error) {
      button.textContent = "Copy failed";
    }
  }

  document.addEventListener("click", handleClick);
})();
