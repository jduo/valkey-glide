use pyo3::ffi;
use pyo3::prelude::*;
use std::os::raw::c_char;

/// CommandResponse layout — must match ffi/src/lib.rs exactly (96 bytes on 64-bit).
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
            // Array
            let len = r.array_value_len as isize;
            let list = ffi::PyList_New(len);
            if list.is_null() { return std::ptr::null_mut(); }
            for i in 0..len {
                let item = convert(r.array_value.offset(i));
                if item.is_null() { ffi::Py_DECREF(list); return std::ptr::null_mut(); }
                ffi::PyList_SetItem(list, i, item); // steals ref
            }
            list
        },
        6 => unsafe {
            // Map
            let len = r.array_value_len as isize;
            let dict = ffi::PyDict_New();
            if dict.is_null() { return std::ptr::null_mut(); }
            for i in 0..len {
                let entry = &*r.array_value.offset(i);
                let key = convert(entry.map_key);
                if key.is_null() { ffi::Py_DECREF(dict); return std::ptr::null_mut(); }
                let val = convert(entry.map_value);
                if val.is_null() { ffi::Py_DECREF(key); ffi::Py_DECREF(dict); return std::ptr::null_mut(); }
                ffi::PyDict_SetItem(dict, key, val);
                ffi::Py_DECREF(key);
                ffi::Py_DECREF(val);
            }
            dict
        },
        7 => unsafe {
            // Set
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
            // OK — return str "OK"
            ffi::PyUnicode_FromStringAndSize(b"OK\0".as_ptr() as *const c_char, 2)
        },
        9 => unsafe {
            // Error — return as bytes, caller handles
            ffi::PyBytes_FromStringAndSize(r.string_value, r.string_value_len as isize)
        },
        _ => unsafe { ffi::Py_NewRef(ffi::Py_None()) },
    }
}

/// Parse a CommandResponse pointer (as integer) into a native Python object.
#[pyfunction]
fn parse_response(py: Python<'_>, ptr: usize) -> PyObject {
    if ptr == 0 {
        return py.None();
    }
    let raw = unsafe { convert(ptr as *const CommandResponse) };
    if raw.is_null() {
        return py.None();
    }
    unsafe { PyObject::from_owned_ptr(py, raw) }
}

#[pymodule]
fn _fast_response(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_response, m)?)?;
    Ok(())
}
