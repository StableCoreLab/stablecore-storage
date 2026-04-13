function(SCEnableUtf8ForTargets)
    foreach(target_name IN LISTS ARGN)
        if (TARGET ${target_name})
            target_compile_options(${target_name} PRIVATE
                $<$<CXX_COMPILER_ID:MSVC>:/utf-8>
                $<$<C_COMPILER_ID:MSVC>:/utf-8>
            )
        endif()
    endforeach()
endfunction()
