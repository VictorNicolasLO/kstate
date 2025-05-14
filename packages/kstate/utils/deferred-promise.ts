
export function createDeferredPromise() {
    let resolve:any, reject: any;
    
    const promise = new Promise((res, rej) => {
        resolve = res;
        reject = rej;
    });

    return { promise, resolve: resolve as (...args:any)=> void, reject: reject as (...args:any)=> void};
}