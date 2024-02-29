# This recursively goes through every mp4 file in the current directory and creates a gif from them, it excludes hidden files
# ${1%.*}.gif takes out the file extension and replaces it with gif
find ./ -name "*.mp4" -type f -not -path '*/.*' -exec sh -c 'ffmpeg -i "$1" -r 15 -vf scale=660:-1 -ss 0 "${1%.*}.gif" -hide_banner' sh {} \;