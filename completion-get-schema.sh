_get_schema_completions()
{
    local cur prev opts types hasdir=0
    COMPREPLY=()
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"
    opts="--type --file --output --delimiter --multiline"

    if [[ ${prev} == "--output" ]]; then
        outputs="stdout"
        COMPREPLY=( $(compgen -W "${outputs}" -- ${cur}) )
        return 0
    fi

    if [[ ${prev} == "--type" ]]; then
        types="json csv parquet"
        COMPREPLY=( $(compgen -W "${types}" -- ${cur}) )
        return 0
    fi

    if [[ ${prev} == "--delimiter" ]]; then
        outputs="'|' ',' ':' '/'"
        COMPREPLY=( $(compgen -W "${outputs}" -- ${cur}) )
        return 0
    fi

    if [[ ${prev} == "--file" ]]; then
        mapfile -t files < <(compgen -f -- "${cur}")
        for i in "${!files[@]}"; do
            if [[ -d "${files[$i]}" ]]; then
                files[$i]="${files[$i]}/"
                hasdir=1
            fi
        done
        COMPREPLY=( "${files[@]}" )
        if (( hasdir )); then
            compopt -o nospace
        fi
        return 0
    fi

    if [[ ${cur} == -* ]]; then
        COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
        return 0
    fi
}

complete -F _get_schema_completions ./get-schema.py
complete -F _get_schema_completions get-schema.py

