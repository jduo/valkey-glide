use pyo3::ffi;
use pyo3::prelude::*;
use std::os::raw::c_char;

/// CommandResponse layout — must match ffi/src/lib.rs exactly.
#[repr(C)]
struct CommandResponse {
    response_type: i32,
    _pad0: i32,
    int_value: i64,
    float_value: f64,
    bool_value: bool,
    _pad1: [u8; 7],
    string_value: *const c_char,
    string_value_len: i64,
    array_value: *const CommandResponse,
    array_value_len: i64,
    map_key: *const CommandResponse,
    map_value: *const CommandResponse,
    sets_value: *const CommandResponse,
    sets_value_len: i64,
    user_data: *mut std::ffi::c_void,
}

unsafe fn convert(resp: *const CommandResponse) -> *mut ffi::PyObject {
    if resp.is_null() {
        return unsafe { ffi::Py_NewRef(ffi::Py_None()) };
    }
    let r = unsafe { &*resp };
    match r.response_type {
        0 => unsafe { ffi::Py_NewRef(ffi::Py_None()) },
        1 => unsafe { ffi::PyLong_FromLongLong(r.int_value) },
        2 => unsafe { ffi::PyFloat_FromDouble(r.float_value) },
        3 => unsafe { ffi::PyBool_FromLong(r.bool_value as std::ffi::c_long) },
        4 => unsafe {
            ffi::PyBytes_FromStringAndSize(r.string_value, r.string_value_len as isize)
        },
        5 => unsafe {
            let len = r.array_value_len as isize;
            let list = ffi::PyList_New(len);
            if list.is_null() { return std::ptr::null_mut(); }
            for i in 0..len {
                let item = convert(r.array_value.offset(i));
                if item.is_null() { ffi::Py_DECREF(list); return std::ptr::null_mut(); }
                ffi::PyList_SetItem(list, i, item);
            }
            list
        },
        6 => unsafe {
            // Map — flat layout: [k0, v0, k1, v1, ...], array_value_len = num entries
            let n = r.array_value_len as isize;
            let dict = ffi::PyDict_New();
            if dict.is_null() { return std::ptr::null_mut(); }
            for i in 0..n {
                let key = convert(r.array_value.offset(i * 2));
                if key.is_null() { ffi::Py_DECREF(dict); return std::ptr::null_mut(); }
                let val = convert(r.array_value.offset(i * 2 + 1));
                if val.is_null() { ffi::Py_DECREF(key); ffi::Py_DECREF(dict); return std::ptr::null_mut(); }
                ffi::PyDict_SetItem(dict, key, val);
                ffi::Py_DECREF(key);
                ffi::Py_DECREF(val);
            }
            dict
        },
        7 => unsafe {
            let len = r.sets_value_len as isize;
            let set = ffi::PySet_New(std::ptr::null_mut());
            if set.is_null() { return std::ptr::null_mut(); }
            for i in 0..len {
                let item = convert(r.sets_value.offset(i));
                if item.is_null() { ffi::Py_DECREF(set); return std::ptr::null_mut(); }
                ffi::PySet_Add(set, item);
                ffi::Py_DECREF(item);
            }
            set
        },
        8 => unsafe {
            ffi::PyUnicode_FromStringAndSize(b"OK\0".as_ptr() as *const c_char, 2)
        },
        9 => unsafe {
            ffi::PyBytes_FromStringAndSize(r.string_value, r.string_value_len as isize)
        },
        _ => unsafe { ffi::Py_NewRef(ffi::Py_None()) },
    }
}

/// Parse a CommandResponse and return (result, arena_ptr).
/// The caller must call free_response_arena(arena_ptr) when done.
#[pyfunction]
fn parse_response(py: Python<'_>, ptr: usize) -> (PyObject, usize) {
    if ptr == 0 {
        return (py.None(), 0);
    }
    let resp = ptr as *const CommandResponse;
    let arena_ptr = unsafe { (*resp).user_data as usize };
    let raw = unsafe { convert(resp) };
    let obj = if raw.is_null() { py.None() } else { unsafe { PyObject::from_owned_ptr(py, raw) } };
    (obj, arena_ptr)
}

#[pymodule]
fn _fast_response(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_response, m)?)?;
    Ok(())
}
