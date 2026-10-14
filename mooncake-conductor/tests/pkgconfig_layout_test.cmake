# SPDX-License-Identifier: Apache-2.0
include("${CMAKE_CURRENT_LIST_DIR}/../cmake/ConductorPkgConfig.cmake")
set(template
    "${CMAKE_CURRENT_LIST_DIR}/../cmake/mooncake-conductor-client.pc.in")
set(CMAKE_INSTALL_PREFIX "${TEST_DIR}/configured-prefix")

function(check_package pc_dir expected_prefix expected_libdir
         expected_includedir)
  foreach(variable IN ITEMS prefix libdir includedir)
    execute_process(
      COMMAND
        "${CMAKE_COMMAND}" -E env "PKG_CONFIG_PATH=${pc_dir}"
        "PKG_CONFIG_LIBDIR=${pc_dir}" "${PKG_CONFIG_EXECUTABLE}"
        "--variable=${variable}" mooncake-conductor-client
      RESULT_VARIABLE status
      OUTPUT_VARIABLE reported
      ERROR_VARIABLE error
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(NOT status EQUAL 0)
      message(FATAL_ERROR "pkg-config ${variable}: ${error}")
    endif()
    get_filename_component(reported "${reported}" ABSOLUTE)
    get_filename_component(expected "${expected_${variable}}" ABSOLUTE)
    if(NOT reported STREQUAL expected)
      message(
        FATAL_ERROR "${pc_dir}: ${variable}=${reported}, expected ${expected}")
    endif()
  endforeach()
endfunction()

foreach(libdir IN ITEMS lib lib64 lib/aarch64-linux-gnu)
  # Consume the real template at an alternate install prefix and then relocate
  # the generated file unchanged; neither consumer overrides its prefix.
  foreach(stage IN ITEMS alternate-prefix relocated)
    set(prefix "${TEST_DIR}/${stage}")
    set(pc_dir "${prefix}/${libdir}/pkgconfig")
    file(MAKE_DIRECTORY "${pc_dir}")
    if(stage STREQUAL "alternate-prefix")
      conductor_configure_pkg_config_file(
        "${template}" "${pc_dir}/mooncake-conductor-client.pc" "${libdir}"
        include)
    else()
      configure_file(
        "${TEST_DIR}/alternate-prefix/${libdir}/pkgconfig/mooncake-conductor-client.pc"
        "${pc_dir}/mooncake-conductor-client.pc"
        COPYONLY)
    endif()
    check_package("${pc_dir}" "${prefix}" "${prefix}/${libdir}"
                  "${prefix}/include")
  endforeach()
endforeach()

# Absolute library/header destinations must not be prefixed a second time.
set(absolute_libdir "${TEST_DIR}/absolute-lib")
set(absolute_includedir "${TEST_DIR}/absolute-include")
conductor_configure_pkg_config_file(
  "${template}" "${absolute_libdir}/pkgconfig/mooncake-conductor-client.pc"
  "${absolute_libdir}" include)
check_package("${absolute_libdir}/pkgconfig" "${CMAKE_INSTALL_PREFIX}"
              "${absolute_libdir}" "${CMAKE_INSTALL_PREFIX}/include")
set(pc_dir "${CMAKE_INSTALL_PREFIX}/lib/pkgconfig")
conductor_configure_pkg_config_file(
  "${template}" "${pc_dir}/mooncake-conductor-client.pc" lib
  "${absolute_includedir}")
check_package("${pc_dir}" "${CMAKE_INSTALL_PREFIX}"
              "${CMAKE_INSTALL_PREFIX}/lib" "${absolute_includedir}")
