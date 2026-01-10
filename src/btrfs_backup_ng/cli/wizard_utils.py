"""Rich-based utilities for interactive configuration wizards.

Provides enhanced prompts, tables, and display functions for a better
user experience in the config wizard.
"""

from pathlib import Path
from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, IntPrompt, Prompt
from rich.style import Style
from rich.table import Table

# Shared console instance
console = Console()


def prompt(message: str, default: str = "") -> str:
    """Prompt user for input with optional default value.

    Args:
        message: Prompt message to display
        default: Default value if user presses Enter

    Returns:
        User input or default value

    Raises:
        KeyboardInterrupt: If user presses Ctrl+C
    """
    try:
        return Prompt.ask(message, default=default, console=console)
    except (EOFError, KeyboardInterrupt):
        console.print()
        raise KeyboardInterrupt


def prompt_bool(message: str, default: bool = True) -> bool:
    """Prompt user for yes/no input.

    Args:
        message: Prompt message to display
        default: Default value if user presses Enter

    Returns:
        Boolean response

    Raises:
        KeyboardInterrupt: If user presses Ctrl+C
    """
    try:
        return Confirm.ask(message, default=default, console=console)
    except (EOFError, KeyboardInterrupt):
        console.print()
        raise KeyboardInterrupt


def prompt_int(message: str, default: int, min_val: int = 0, max_val: int = 100) -> int:
    """Prompt user for integer input with validation.

    Args:
        message: Prompt message to display
        default: Default value if user presses Enter
        min_val: Minimum allowed value
        max_val: Maximum allowed value

    Returns:
        Integer response within valid range

    Raises:
        KeyboardInterrupt: If user presses Ctrl+C
    """
    while True:
        try:
            value = IntPrompt.ask(message, default=default, console=console)
            if min_val <= value <= max_val:
                return value
            console.print(
                f"  [yellow]Value must be between {min_val} and {max_val}[/yellow]"
            )
        except (EOFError, KeyboardInterrupt):
            console.print()
            raise KeyboardInterrupt


def prompt_choice(message: str, choices: list[str], default: str = "") -> str:
    """Prompt user to select from a list of choices.

    Args:
        message: Prompt message to display
        choices: List of valid choices
        default: Default choice if user presses Enter

    Returns:
        Selected choice

    Raises:
        KeyboardInterrupt: If user presses Ctrl+C
    """
    # Build a table of choices
    table = Table(show_header=False, box=None, padding=(0, 2))
    table.add_column("Num", style="cyan", justify="right")
    table.add_column("Choice")

    for i, choice in enumerate(choices, 1):
        marker = " [green](default)[/green]" if choice == default else ""
        table.add_row(f"{i}.", f"{choice}{marker}")

    console.print()
    console.print(message)
    console.print(table)
    console.print()

    while True:
        try:
            value = Prompt.ask(
                "Enter number or value",
                default=default if default else None,
                console=console,
            )
        except (EOFError, KeyboardInterrupt):
            console.print()
            raise KeyboardInterrupt

        if not value:
            if default:
                return default
            continue

        # Try as number
        try:
            idx = int(value) - 1
            if 0 <= idx < len(choices):
                return choices[idx]
        except ValueError:
            pass

        # Try as direct value
        if value in choices:
            return value

        console.print(
            f"  [yellow]Invalid choice. Enter 1-{len(choices)} "
            f"or a value from the list.[/yellow]"
        )


def prompt_selection(
    title: str,
    items: list[dict[str, Any]],
    columns: list[tuple[str, str]],
    default_indices: list[int] | None = None,
    recommended_indices: list[int] | None = None,
    footer_notes: list[str] | None = None,
) -> list[int]:
    """Display a table and prompt user to select items.

    Args:
        title: Table title
        items: List of item dicts with keys matching column keys
        columns: List of (key, header) tuples defining columns
        default_indices: Indices selected by default (0-based)
        recommended_indices: Indices to mark as recommended (0-based)
        footer_notes: Additional notes to display below the table

    Returns:
        List of selected indices (0-based)

    Raises:
        KeyboardInterrupt: If user presses Ctrl+C
    """
    if default_indices is None:
        default_indices = []
    if recommended_indices is None:
        recommended_indices = []

    # Build the table
    table = Table(title=title, show_header=True, header_style="bold")
    table.add_column("#", style="cyan", justify="right", width=4)
    table.add_column("", width=3)  # Recommendation marker

    for key, header in columns:
        table.add_column(header)

    for i, item in enumerate(items):
        num = str(i + 1)
        marker = "[green]★[/green]" if i in recommended_indices else ""

        row = [num, marker]
        for key, _ in columns:
            value = item.get(key, "")
            row.append(str(value) if value else "-")

        # Style recommended rows
        style = None
        if i in recommended_indices:
            style = Style(bold=True)

        table.add_row(*row, style=style)

    console.print()
    console.print(table)

    if recommended_indices:
        console.print("  [green]★[/green] = recommended for backup")
    if footer_notes:
        for note in footer_notes:
            console.print(f"  {note}")
    console.print()

    # Build default selection string
    if default_indices:
        default_str = ",".join(str(i + 1) for i in default_indices)
    else:
        default_str = ""

    while True:
        try:
            selection = Prompt.ask(
                f"Select items [dim](1-{len(items)}, comma-separated, or 'all')[/dim]",
                default=default_str if default_str else None,
                console=console,
            )
        except (EOFError, KeyboardInterrupt):
            console.print()
            raise KeyboardInterrupt

        if not selection:
            if default_indices:
                return default_indices
            continue

        # Handle 'all'
        if selection.lower() == "all":
            return list(range(len(items)))

        # Parse comma-separated numbers
        try:
            selected = []
            for part in selection.split(","):
                part = part.strip()
                if not part:
                    continue
                idx = int(part) - 1
                if 0 <= idx < len(items):
                    if idx not in selected:
                        selected.append(idx)
                else:
                    console.print(f"  [yellow]'{part}' is not valid, skipping[/yellow]")

            if selected:
                return selected
            console.print("  [yellow]No valid selections. Try again.[/yellow]")
        except ValueError:
            console.print(
                f"  [yellow]Invalid input. Enter numbers 1-{len(items)}, "
                "comma-separated, or 'all'.[/yellow]"
            )


def display_snapper_configs(configs: list) -> None:
    """Display detected snapper configurations in a table.

    Args:
        configs: List of SnapperConfig objects
    """
    if not configs:
        return

    table = Table(title="Detected Snapper Configurations", show_header=True)
    table.add_column("#", style="cyan", justify="right", width=4)
    table.add_column("Name", style="green")
    table.add_column("Subvolume")

    for i, cfg in enumerate(configs, 1):
        table.add_row(str(i), cfg.name, str(cfg.subvolume))

    console.print()
    console.print(table)
    console.print()


def display_btrbk_import(volumes: list[dict], warnings: list[str]) -> None:
    """Display imported btrbk configuration.

    Args:
        volumes: List of volume dicts with 'path' and 'targets' keys
        warnings: List of import warnings
    """
    # Show warnings first if any
    if warnings:
        warning_text = "\n".join(f"  [yellow]![/yellow] {w}" for w in warnings)
        console.print(
            Panel(
                warning_text,
                title="[yellow]Import Warnings[/yellow]",
                border_style="yellow",
            )
        )
        console.print()

    # Show volumes table
    table = Table(title="Imported from btrbk", show_header=True)
    table.add_column("Volume", style="green")
    table.add_column("Targets")

    for vol in volumes:
        path = vol.get("path", "")
        targets = vol.get("targets", [])
        target_strs = [t.get("path", "") for t in targets]

        # First row with volume path
        if target_strs:
            table.add_row(path, target_strs[0])
            # Additional targets
            for t in target_strs[1:]:
                table.add_row("", t)
        else:
            table.add_row(path, "[dim]No targets[/dim]")

    console.print(table)
    console.print()


def display_config_preview(config_toml: str) -> None:
    """Display a preview of the generated configuration.

    Args:
        config_toml: TOML configuration content
    """
    console.print(
        Panel(
            config_toml,
            title="Configuration Preview",
            border_style="blue",
            expand=False,
        )
    )


def display_wizard_header(title: str, subtitle: str = "") -> None:
    """Display a wizard section header.

    Args:
        title: Main title text
        subtitle: Optional subtitle/description
    """
    content = f"[bold]{title}[/bold]"
    if subtitle:
        content += f"\n[dim]{subtitle}[/dim]"

    console.print()
    console.print(Panel(content, border_style="blue"))
    console.print()


def display_section_header(title: str) -> None:
    """Display a section header within the wizard.

    Args:
        title: Section title
    """
    console.print()
    console.print(f"[bold cyan]── {title} ──[/bold cyan]")
    console.print()


def display_next_steps(steps: list[str]) -> None:
    """Display next steps after wizard completion.

    Args:
        steps: List of step descriptions
    """
    content = "\n".join(f"  {i}. {step}" for i, step in enumerate(steps, 1))
    console.print(
        Panel(
            content,
            title="[green]Next Steps[/green]",
            border_style="green",
        )
    )


def find_btrbk_config() -> Path | None:
    """Check for existing btrbk configuration files.

    Returns:
        Path to btrbk config if found, None otherwise
    """
    standard_locations = [
        Path("/etc/btrbk/btrbk.conf"),
        Path("/etc/btrbk.conf"),
    ]

    for path in standard_locations:
        if path.exists():
            return path

    return None


def display_btrbk_detected(btrbk_path: Path) -> str:
    """Display btrbk detection and prompt for action.

    Args:
        btrbk_path: Path to detected btrbk config

    Returns:
        User's choice: 'import', 'detect', or 'manual'
    """
    console.print(
        Panel(
            f"[green]Found:[/green] {btrbk_path}\n\n"
            "You can import your existing btrbk configuration\n"
            "or start fresh with auto-detection.",
            title="[bold]Existing btrbk Configuration Detected[/bold]",
            border_style="green",
        )
    )
    console.print()

    return prompt_choice(
        "How would you like to proceed?",
        ["import", "detect", "manual"],
        default="import",
    )
