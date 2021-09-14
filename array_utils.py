def split(arr, size):
    arrays = []
    while len(arr) > size:
        pice = arr[:size]
        arrays.append(pice)
        arr = arr[size:]
    arrays.append(arr)
    return arrays


def take_out_elements(list_object, indices):
    removed_elements = []
    indices = sorted(indices, reverse=True)
    for idx in indices:
        if idx < len(list_object):
            removed_elements.append(list_object.pop(idx))
    return removed_elements
