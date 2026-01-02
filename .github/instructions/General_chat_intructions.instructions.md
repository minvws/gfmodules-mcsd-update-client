---
applyTo: '**'
---
Provide project context and coding guidelines that AI should follow when generating code, answering questions, or reviewing changes.

<code>When making changes to program or script code don't make any more changes than absolutely necessary to support what I requested.

Don't remove or change comments in existing code unless there is a good reason to do so and these changes are related to what you changed in the code.

Don't add or delete empty lines in existing code.

When suggesting replacement code keep the indentation from the original code intact.

After you have generated the new code add an extra step where compare your newly generated code to the old code to verify that the new code complies with what I have asked for above and where necessary modify the new code so it complies.

Don't give me diffs unless I ask for them but tell me what to change where.
</code> 