let currentPage = 1;
const pageCount = document.querySelector('meta[name="page-count"]');
const pageCountContent = parseInt(pageCount.content);

document.addEventListener("DOMContentLoaded", function() {
    window.scrollTo({top: 0, behavior: "smooth"});
    const search = document.getElementById("search-input");
    const searchValue = search.value.trim()

    if (window.location.pathname.startsWith("/search")) {
        document.querySelector("#all").style.fontWeight = "Bold";
        document.querySelector("#all").style.color = "rebeccapurple";
        document.querySelector("#images").style.fontWeight = "Normal";

        searchPage(searchValue);
    } else if (window.location.pathname.startsWith("/images")) {
        document.querySelector("#images").style.fontWeight = "Bold";
        document.querySelector("#images").style.color = "rebeccapurple";
        document.querySelector("#all").style.fontWeight = "Normal";
        
        imagePage();
    }

    document.addEventListener("click", function(event) {
        if (event.target.id === "all") {
            event.preventDefault();

            window.location.href = `/search?search=${encodeURIComponent(searchValue)}`;
        } else if (event.target.id === "images") {
            event.preventDefault();

            window.location.href = `/images?search=${encodeURIComponent(searchValue)}`;
        }
    })
})

function imagePage() {
    window.onscroll = () => {
        // Infinite scroll with a buffer of 1
        // Buffer prevents the infinite scroll from breaking
        if (window.innerHeight + window.scrollY >= document.documentElement.offsetHeight - 1) {
            currentPage++;
            loadImages(currentPage);
        }
    }
}

function loadImages(page) {
    fetch('/images', {
        method: "POST",
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            page: page
        })
    })
    .then((response) => response.json())
    .then((result) => {
        for (let i = 0; i < result["data"].length; i++) {
            document.querySelector("#image-display").innerHTML += `
                <div class="image-content">
                    <a href="${result["data"][i][0]}"><img src="${result["data"][i][0]}" alt="${result["data"][i][1]}"></a>
                    <a href="${result["data"][i][2]}" class="image-desc">${result["data"][i][2].slice(0, 35)}...</a>
                </div>
            `;
        }
    })
}

function searchPage(search) {
    document.addEventListener("click", function(event) {
        if (event.target.matches("#beginning-arrow")) {
            nextPage(1, search)
        } else if (event.target.matches("#end-arrow")) {
            nextPage(pageCountContent, search)
        } else if (event.target.matches("#page-1")) {
            if (parseInt(event.target.innerHTML) === 1) {
                nextPage(1, search);
            } else if (parseInt(event.target.innerHtml) === pageCountContent) {
                nextPage(currentPage - 2, search);
            } else {
                nextPage(currentPage - 1, search);
            }
        } else if (event.target.matches("#page-2")) {
            if (parseInt(event.target.innerHTML) === currentPage) {
                window.location.reload();
            } else if (parseInt(event.target.innerHTML) > currentPage) {
                nextPage(currentPage + 1, search);
            } else if (parseInt(event.target.innerHTML) < currentPage) {
                nextPage(currentPage - 1, search);
            }
        } else if (event.target.matches("#page-3")) {
            if (parseInt(event.target.innerHTML) === currentPage) {
                window.location.reload();
            } else if (parseInt(event.target.innerHTML) > currentPage + 1) {
                nextPage(currentPage + 2, search);
            } else if (parseInt(event.target.innerHTML) > currentPage) {
                nextPage(currentPage + 1, search);
            }
        }
    })
}

function nextPage(page, search) {
    fetch(`search?search=${encodeURIComponent(search)}&page=${page}`, {
        method: "POST"
    })
    .then((response) => response.json())
    .then((result) => {
        document.querySelector("#results-display").innerHTML = "";
        result.data.forEach((result) => {
            document.querySelector("#results-display").innerHTML += `
                <div class="search-result p-2">
                    <h4>
                        <a id="search-url" class="link-offset-2 link-offset-3-hover link-underline link-underline-opacity-0" href="${result[1]}">${result[0]}</a>
                    </h4>
                    <p class="text-body-secondary">${result[2]}</p>
                </div>
            `;
        })
        currentPage = page;
        window.scrollTo({top: 0, behavior: "smooth"});
        createPagination(currentPage)
    })
}

function createPagination(page) {
    let paginationHTML = document.querySelector(".pagination").innerHTML
    if (page === 1) {
        paginationHTML = `<li class="page-item"><button class="page-link" id="beginning-arrow" disabled><<</button></li>
            <li class="page-item"><button class="page-link active" id="page-1">${page}</button></li>
            ${pageCountContent > 1 ? `<li class="page-item"><button class="page-link" id="page-2">${page + 1}</button></li>` : ''}
            ${pageCountContent > 2 ? `<li class="page-item"><button class="page-link" id="page-3">${page + 2}</button></li>` : ''}
            <li class="page-item"><button class="page-link" id="end-arrow" ${pageCountContent > 1 ? '' : 'disabled'}>>></button></li>`;
    } else if (page === pageCountContent) {
        paginationHTML = `<li class="page-item"><button class="page-link" id="beginning-arrow"><<</button></li>
            ${pageCountContent > 2 ? `<li class="page-item"><button class="page-link" id="page-1">${page - 2}</button></li>` : ''}
            <li class="page-item"><button class="page-link" id="page-2">${page - 1}</button></li>
            <li class="page-item"><button class="page-link active" id="page-3">${page}</button></li>
            <li class="page-item"><button class="page-link" id="end-arrow" disabled>>></button></li>`;
    } else if (page > 1 && page < pageCountContent) {
        paginationHTML = `<li class="page-item"><button class="page-link" id="beginning-arrow"><<</button></li>
            <li class="page-item"><button class="page-link" id="page-1">${page - 1}</button></li>
            <li class="page-item"><button class="page-link active" id="page-2">${page}</button></li>
            <li class="page-item"><button class="page-link" id="page-3">${page + 1}</button></li>
            <li class="page-item"><button class="page-link" id="end-arrow">>></button></li>`;
    }

    document.querySelector(".pagination").innerHTML = paginationHTML;
}