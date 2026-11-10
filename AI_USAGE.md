# AI assistance disclosure

On 2026-11-10, at Ali Hadi Zbeeb's request, OpenAI Codex generated the initial
Person 2 implementation in `window.py`, the tests in `tests/`, the demonstration
in `demo_person2.py`, `.gitignore`, README, and the Person 2 explanation guide.
Codex also executed the test suite and demonstration scenarios in its workspace.
The existing contract was read as the interface specification and was not changed.

Ali is the assigned component owner. Comments naming Ali identify that assignment;
they do not claim that he independently authored or reviewed this generated code.
No student review, adaptation, or teammate approval is asserted here.

The project brief requires the bulk of submitted code to be developed/generated
by the students and AI-generated code to be clearly documented. This is an
AI-assisted implementation draft for study, testing, and student development.
Keep the generated origin disclosed; record actual student modifications and
understanding/review as they happen. Do not describe unchanged generated code as
independently student-written.

Design references:

- `CONTRACT.md`, version 1.0 proposed baseline, 2026-11-08.
- EECE 350 project brief: Person 2 responsibilities and deliverables.
- RFC 6298, sections 2, 3 and 5: https://www.rfc-editor.org/rfc/rfc6298.html

The implementation was written for this contract; no third-party implementation
was copied and no third-party package is needed. The timeout is TCP-inspired;
this project is not a complete TCP implementation.
