#!/bin/bash

fname=$1
echo=/bin/echo

${echo} "#ifndef MANGLED_H"
${echo} "#define MANGLED_H"
${echo}

${echo} -n "#define STACK_FRAME_SPLIT_RETURN_MANGLED_NAME "
grep '^_.*split_return.*:$' ${fname} | grep -v PRETTY | sed -e 's/:.*$/"/;s/^/"/' | head -n 1
${echo}

${echo} -n "#define STACK_FRAME_SPLIT_CTRL_MANGLED_NAME "
grep '^_.*split_ctrl[^_].*:$' ${fname} | grep -v PRETTY | sed -e 's/:.*$/"/;s/^/"/' | head -n 1
${echo}

${echo} -n "#define STACK_FRAME_SPLIT_CTRL_EXECUTING_MANGLED_NAME "
grep '^_.*split_ctrl_executing.*:$' ${fname} | grep -v PRETTY | sed -e 's/:.*$/"/;s/^/"/' | head -n 1
${echo}

${echo} -n "#define STACK_FRAME_SPLIT_CTRL_WAITING_MANGLED_NAME "
grep '^_.*split_ctrl_waiting.*:$' ${fname} | grep -v PRETTY | sed -e 's/:.*$/"/;s/^/"/' | head -n 1
${echo}

${echo} -n "#define STACK_FRAME_SPLIT_CTRL_PENDING_MANGLED_NAME "
grep '^_.*split_ctrl_pending.*:$' ${fname} | grep -v PRETTY | sed -e 's/:.*$/"/;s/^/"/' | head -n 1
${echo}

${echo} "#endif // MANGLED_H"
