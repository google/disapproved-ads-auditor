# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

def split(arr, size):
    """Splits an array to smaller arrays of size"""
    arrays = []
    while len(arr) > size:
        piece = arr[:size]
        arrays.append(piece)
        arr = arr[size:]
    arrays.append(arr)
    return arrays


def take_out_elements(list_object, indices):
    """Removes elements from list in specified indices"""
    removed_elements = []
    indices = sorted(indices, reverse=True)
    for idx in indices:
        if idx < len(list_object):
            removed_elements.append(list_object.pop(idx))
    return removed_elements
