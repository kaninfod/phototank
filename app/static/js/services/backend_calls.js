



export async function setRating(guid, rating) {
  const resp = await fetch('/phototank/rate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ guid, rating }),
  })
  
  if (!resp.ok) {
    throw new Error(`Rating update failed: ${resp.statusText}`)
  }
  
  return await resp.json()
}

export async function addTags(guids, tag) {
    const url = `/phototank/tags/${tag}/apply`
    const payload = { guids: guids }
    console .log("Adding tag with payload:", payload)
    const resp = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
    })
    
    if (!resp.ok) {
        throw new Error(`Tag update failed: ${resp.statusText}`)
    }
    
    return await resp.json()
}

export async function removeTags(guids, tag) {
    const url = `/phototank/tags/${tag}/remove`
    const payload = { guids: guids }
    const resp = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
    })
    
    if (!resp.ok) {
        throw new Error(`Tag update failed: ${resp.statusText}`)
    }
    
    return await resp.json()
}

export async function addNewTag(payload) {
    const url = '/phototank/tags'
    const resp = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
    })
    
    if (!resp.ok) {
        throw new Error(`Tag creation failed: ${resp.statusText}`)
    }
    
    return await resp.json()
}

export async function serverRequest(url, type, payload) {
    const resp = await fetch(url, {
        method: type,
        headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
        });

        let responsePayload = null;
        try { 
            responsePayload = await resp.json(); 
        } catch { 
            console.error('Failed to parse response JSON for request to ', url);
        }


        if (!resp.ok) {
            console.error(`Error with request to ${url}: ${resp.statusText}`);
            throw new Error(resp.statusText);
        } else {
            console.log(`Request to ${url} successful`, responsePayload);
            return responsePayload
        }
    }   