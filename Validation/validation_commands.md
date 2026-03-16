## Count files
This will do the equivalent to going to properties and getting the file count but it runs way faster and is more accurate.

(Get-ChildItem -Path "W:\papers\current\french_records\french_record_images\Creuse" -Recurse -File -Force -ErrorAction SilentlyContinue).Count