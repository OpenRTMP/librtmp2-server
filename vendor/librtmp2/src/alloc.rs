//! Custom allocator hooks for librtmp2
//!
//! Mirrors `src/core/alloc.h` and `src/core/alloc.c`.
//! Provides pluggable allocation functions with standard defaults.


/// Allocator function type.
pub type AllocFn = fn(size: usize, userdata: *mut u8) -> *mut u8;
/// Reallocator function type.
pub type ReallocFn = fn(ptr: *mut u8, size: usize, userdata: *mut u8) -> *mut u8;
/// Free function type.
pub type FreeFn = fn(ptr: *mut u8, userdata: *mut u8);

static mut G_ALLOC: AllocFn = std_alloc;
static mut G_REALLOC: ReallocFn = std_realloc;
static mut G_FREE: FreeFn = std_free;
static mut G_USERDATA: *mut u8 = std::ptr::null_mut();

fn std_alloc(size: usize, _ud: *mut u8) -> *mut u8 {
    if size == 0 {
        return std::ptr::null_mut();
    }
    unsafe { libc::malloc(size) as *mut u8 }
}

fn std_realloc(ptr: *mut u8, size: usize, _ud: *mut u8) -> *mut u8 {
    if ptr.is_null() {
        return std_alloc(size, _ud);
    }
    if size == 0 {
        std_free(ptr, _ud);
        return std::ptr::null_mut();
    }
    unsafe { libc::realloc(ptr as *mut libc::c_void, size) as *mut u8 }
}

fn std_free(ptr: *mut u8, _ud: *mut u8) {
    if !ptr.is_null() {
        unsafe { libc::free(ptr as *mut libc::c_void) }
    }
}

/// Set custom allocator functions.
pub fn set_allocator(alloc: AllocFn, realloc: ReallocFn, free: FreeFn, userdata: *mut u8) {
    unsafe {
        G_ALLOC = alloc;
        G_REALLOC = realloc;
        G_FREE = free;
        G_USERDATA = userdata;
    }
}

/// Allocate `size` bytes using the current allocator.
pub fn lrtmp2_malloc(size: usize) -> *mut u8 {
    unsafe { G_ALLOC(size, G_USERDATA) }
}

/// Allocate zeroed memory for `nmemb` elements of `size` bytes each.
pub fn lrtmp2_calloc(nmemb: usize, size: usize) -> *mut u8 {
    if nmemb != 0 && size > usize::MAX / nmemb {
        return std::ptr::null_mut();
    }
    let total = nmemb * size;
    let p = lrtmp2_malloc(total);
    if !p.is_null() {
        unsafe {
            std::ptr::write_bytes(p, 0, total);
        }
    }
    p
}

/// Reallocate memory.
pub fn lrtmp2_realloc(ptr: *mut u8, size: usize) -> *mut u8 {
    unsafe { G_REALLOC(ptr, size, G_USERDATA) }
}

/// Free memory.
pub fn lrtmp2_free(ptr: *mut u8) {
    if !ptr.is_null() {
        unsafe {
            G_FREE(ptr, G_USERDATA);
        }
    }
}

/// Allocate a Vec-backed buffer (idiomatic Rust helper).
pub fn alloc_vec<T: Copy + Default>(n: usize) -> Vec<T> {
    vec![T::default(); n]
}
